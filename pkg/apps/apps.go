package apps

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/cjoudrey/gluahttp"
	"github.com/fsnotify/fsnotify"
	"github.com/gorilla/mux"
	log "github.com/inconshreveable/log15"
	"github.com/yuin/gopher-lua"
	_ "golang.org/x/net/context"

	luamod "github.com/tsileo/blobstash/pkg/apps/lua"
	_ "github.com/tsileo/blobstash/pkg/ctxutil"
	"github.com/tsileo/blobstash/pkg/httputil"
)

type Apps struct {
	apps map[string]*App
	log  log.Logger
}

func (apps *Apps) Close() error {
	return nil
}

func (app *App) visit(path string, f os.FileInfo, err error) error {
	p, err := filepath.Rel(app.path, path)
	if err != nil {
		return err
	}
	fmt.Printf("Visited: %s\n", p)
	app.index[p] = f
	return nil
}

func (app *App) reload() error {
	app.index = map[string]os.FileInfo{}
	if err := filepath.Walk(app.path, app.visit); err != nil {
		return err
	}
	return nil
}

func (app *App) watch() {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		panic(err)
	}
	defer watcher.Close()

	done := make(chan bool)
	go func() {
		for {
			select {
			case <-watcher.Events:
				// log.Println("event:", event)
				// if event.Op&fsnotify.Write == fsnotify.Write {
				// log.Println("modified file:", event.Name)
				if err := app.reload(); err != nil {
					panic(err)
					// log.Println("failed to reload:", err)
				}
				// }
			case err := <-watcher.Errors:
				panic(err)
				// log.Println("error:", err)
			}
		}
	}()

	err = watcher.Add(app.path)
	if err != nil {
		panic(err)
		// log.Fatal(err)
	}
	<-done
}

type App struct {
	path  string
	index map[string]os.FileInfo
	log   log.Logger
	mu    sync.Mutex
}

// TODO(tsileo): handle this in the YAML file
type AppConfig struct {
	Name       string
	Path       string
	Entrypoint string
	Domain     string
	Auth       string
}

func (apps *Apps) newApp(path string) (*App, error) {
	app := &App{path, map[string]os.FileInfo{}, apps.log.New("app", path), sync.Mutex{}}
	app.log.Debug("new app")
	return app, app.reload()
}

func (app *App) doLua(script string, r *http.Request, w http.ResponseWriter) error {
	start := time.Now()
	L := lua.NewState()
	defer L.Close()

	L.SetGlobal("unix", L.NewFunction(func(L *lua.LState) int {
		L.Push(lua.LNumber(time.Now().Unix()))
		return 1
	}))

	L.SetGlobal("print", L.NewFunction(func(L *lua.LState) int {
		// TODO(tsileo): get the number of items in L, put them in a slice, and call fmt.Sprintf
		app.log.Info(L.ToString(1))
		return 0
	}))

	// TODO(tsileo): implement a deploy endpoint that take a zipfile and load the app with a config
	// XXX(tsileo): css preprocessing? gocss/less/saas?

	// TODO(tsileo): handle basic auth/api key via config and implement it as a middleware
	// TODO(tsileo): blobstore, kvstore, docstore, filetree module
	// TODO(tsileo): bewit module
	// TODO(tsileo): build a tiny kv wrapper for temp data, e.g. link shortener
	// TODO(tsileo): re-import request, and build a tiny "json" module using luautil
	L.PreloadModule("http", gluahttp.NewHttpModule(&http.Client{}).Loader)

	// Load the blobstash module
	response := luamod.NewResponseModule()
	L.PreloadModule("response", response.Loader)

	fs := luamod.NewFSModule(app.path)
	L.PreloadModule("fs", fs.Loader)

	mustache := luamod.NewMustacheModule()
	L.PreloadModule("mustache", mustache.Loader)

	// Execute the script
	if err := L.DoString(script); err != nil {
		// TODO(tsileo): enable caching with TTL
		// FIXME(tsileo): better error, with debug mode?
		panic(err)
	}
	response.WriteTo(w)
	app.log.Info("script executed", "time", time.Since(start))
	return nil
}

var app0 *App

func New(logger log.Logger) (*Apps, error) {
	var err error
	apps := &Apps{
		apps: map[string]*App{},
		log:  logger,
	}
	app0, err = apps.newApp("/Users/thomas/gopath/src/github.com/tsileo/blobstash/lol")
	if err != nil {
		return nil, err
	}
	apps.apps["0"] = app0
	go app0.watch()
	// "0": app0,
	return apps, nil
}

func HelloServer(w http.ResponseWriter, req *http.Request) {
	// FIXME(tsileo): add the subdomain in the context, and use it in the request handler
	vars := mux.Vars(req)
	fmt.Printf("%+v", vars)
	subdomain := vars["subdomain"]
	p := req.URL.Path
	if p == "/" {
		// TODO(tsileo): find a way to do directory listing?
		p = "/index.html"
	}
	if fi, ok := app0.index[p[1:]]; ok {
		f, err := os.Open(filepath.Join(app0.path, p))
		defer f.Close()
		if err != nil {
			panic(err)
		}
		if strings.HasSuffix(p, ".lua") {
			// TODO(tsileo): handle caching
			script, err := ioutil.ReadAll(f)
			if err != nil {
				panic(err)
			}
			app0.doLua(string(script), req, w)
			return
		}
		if fi.IsDir() {
			fis, err := f.Readdir(-1)
			if err != nil {
				panic(err)
			}
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			fmt.Fprintf(w, "<!doctype html><title>BlobStash - %s</title><pre>\n", fi.Name())

			// TODO(tsileo) better root check
			if p[1:] != "index.html" {
				p := filepath.Dir(fi.Name())
				fmt.Fprintf(w, "<a href=\"%s\">%s</a>\n", p, "..")
			}
			for _, cfi := range fis {
				p := filepath.Join(fi.Name(), cfi.Name())
				fmt.Fprintf(w, "<a href=\"%s\">%s</a>\n", p, cfi.Name())
			}
			fmt.Fprintf(w, "</pre>\n")
			return
		}

		// The node is a file
		fmt.Printf("fi=%+v\n", fi)
		httputil.SetAttachment(fi.Name(), req, w)
		// TODO(tsileo): support resizing
		http.ServeContent(w, req, fi.Name(), fi.ModTime(), f)
		return
		// TODO(tsileo): returns a 404
	}
	fmt.Printf("p=%v\n", p)
	io.WriteString(w, "hello, "+subdomain)
}

func (apps *Apps) Register(r *mux.Router, root *mux.Router, basicAuth func(http.Handler) http.Handler) {
	// root.Host("{subdomain}.a4.io").Path("/").HandlerFunc(HelloServer)
	root.Host("{subdomain}.a4.io").HandlerFunc(HelloServer)
}
