package apps

import (
	"fmt"
	_ "io"
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
	"github.com/tsileo/blobstash/pkg/apps/luautil"
	"github.com/tsileo/blobstash/pkg/config"
	_ "github.com/tsileo/blobstash/pkg/ctxutil"
	"github.com/tsileo/blobstash/pkg/httputil"
)

type Apps struct {
	apps   map[string]*App
	config *config.Config
	log    log.Logger
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
	path, name string
	domain     string
	config     map[string]interface{}

	index map[string]os.FileInfo
	log   log.Logger
	mu    sync.Mutex
}

func (apps *Apps) newApp(appConf *config.AppConfig) (*App, error) {
	app := &App{
		path:   appConf.Path,
		name:   appConf.Name,
		domain: appConf.Domain,
		config: appConf.Config,
		index:  map[string]os.FileInfo{},
		log:    apps.log.New("app", appConf.Name),
		mu:     sync.Mutex{},
	}
	// TODO(tsileo): check that `path` exists, create it if it doesn't exist?
	app.log.Debug("new app")
	return app, app.reload()
}

// Serve the request for the given path
func (app *App) serve(path string, w http.ResponseWriter, req *http.Request) {
	// if fi, ok := app0.index[p[1:]]; ok {
	// 	f, err := os.Open(filepath.Join(app0.path, p))
	if fi, ok := app.index[path]; ok {
		// Open the file
		f, err := os.Open(filepath.Join(app.path, path))
		defer f.Close()
		if err != nil {
			panic(err)
		}

		// The node is a Lua script, execute it
		if strings.HasSuffix(path, ".lua") {
			// TODO(tsileo): handle caching
			script, err := ioutil.ReadAll(f)
			if err != nil {
				panic(err)
			}
			app.doLua(string(script), req, w)
			return
		}

		// The node is a dir, display the file content in a really basic HTML page
		if fi.IsDir() {
			fis, err := f.Readdir(-1)
			if err != nil {
				panic(err)
			}
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			fmt.Fprintf(w, "<!doctype html><title>BlobStash - %s</title><pre>\n", fi.Name())

			// TODO(tsileo) better root check
			if path != "index.html" {
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
	}
	// TODO(tsileo): returns a 404
	// fmt.Printf("p=%v\n", p)
	// io.WriteString(w, "hello, "+subdomain)
}

// Execute the Lua script contained in the script
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

	// Make the config map defined in the config available from the script
	L.SetGlobal("config", luautil.InterfaceToLValue(L, app.config))

	// XXX(tsileo): css preprocessing? gocss/less/saas?

	// TODO(tsileo): handle basic auth/api key via config and implement it as a middleware
	// TODO(tsileo): blobstore, kvstore, docstore, filetree module
	// TODO(tsileo): bewit module
	// TODO(tsileo): build a tiny kv wrapper for temp data, e.g. link shortener
	// TODO(tsileo): build a tiny "json" module using luautil
	L.PreloadModule("http", gluahttp.NewHttpModule(&http.Client{}).Loader)

	// Load the blobstash module
	response := luamod.NewResponseModule()
	L.PreloadModule("response", response.Loader)

	request := luamod.NewRequestModule(r)
	L.PreloadModule("request", request.Loader)

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

// var app0 *App

func New(logger log.Logger, conf *config.Config) (*Apps, error) {
	// var err error
	apps := &Apps{
		apps:   map[string]*App{},
		log:    logger,
		config: conf,
	}
	for _, appConf := range conf.Apps {
		app, err := apps.newApp(appConf)
		if err != nil {
			return nil, err
		}
		fmt.Printf("app %+v\n", app)
		apps.apps[app.name] = app
		// FIXME(tsileo): register the root.Host(config.domain... here, or do it in the register call
		go app.watch()
		// app0 = app
	}
	return apps, nil
}

func (apps *Apps) appHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	// First, find which app we're trying to call
	appName := vars["name"]
	// => select the app and call its handler?
	app, ok := apps.apps[appName]
	if !ok {
		// FIXME(tsileo): 404 here
		return
	}
	// Check that the requested file/path exists
	// p := req.URL.Path
	p := vars["path"]
	fmt.Printf("URL PATH=%+v\nAPP=%+v", p, app)
	if p == "/" {
		// TODO(tsileo): find a way to do directory listing?
		p = "/index.html"
	}
	app.serve(p, w, req)
}

// func (apps *Apps) subdomainHandler(w http.ResponseWriter, req *http.Request) {
// 	// FIXME(tsileo): add the subdomain in the context, and use it in the request handler
// 	// First, find which app we're trying to call

// 	// => select the app and call its handler?

// 	// Check that the requested file/path exists
// 	vars := mux.Vars(req)
// 	fmt.Printf("%+v", vars)
// 	subdomain := vars["subdomain"]
// 	p := req.URL.Path
// 	if p == "/" {
// 		// TODO(tsileo): find a way to do directory listing?
// 		p = "/index.html"
// 	}
// }

func (apps *Apps) Register(r *mux.Router, root *mux.Router, basicAuth func(http.Handler) http.Handler) {
	// root.Host("{subdomain}.a4.io").Path("/").HandlerFunc(HelloServer)
	r.Handle("/{name}/", http.HandlerFunc(apps.appHandler))
	r.Handle("/{name}/{path:.+}", http.HandlerFunc(apps.appHandler))
	// XXX(tsileo): with custom domain handling, we don't need the routing?? just run app.serve??
	// root.Host("{subdomain}.a4.io").HandlerFunc(apps.handler)
}
