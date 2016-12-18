package apps

import (
	"crypto/md5"
	"fmt"
	_ "io"
	"io/ioutil"
	"net/http"
	rhttputil "net/http/httputil"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/cjoudrey/gluahttp"
	"github.com/fsnotify/fsnotify"
	"github.com/gorilla/mux"
	log "github.com/inconshreveable/log15"
	"github.com/yuin/gopher-lua"
	"golang.org/x/net/context"

	luamod "github.com/tsileo/blobstash/pkg/apps/lua"
	"github.com/tsileo/blobstash/pkg/apps/luautil"
	"github.com/tsileo/blobstash/pkg/blob"
	"github.com/tsileo/blobstash/pkg/config"
	_ "github.com/tsileo/blobstash/pkg/ctxutil"
	"github.com/tsileo/blobstash/pkg/filetree"
	"github.com/tsileo/blobstash/pkg/httputil"
	"github.com/tsileo/blobstash/pkg/hub"
)

// TODO(tsileo): at startup, scan all filetree FS and looks for app.yaml for registering

// Apps holds the Apps manager data
type Apps struct {
	apps   map[string]*App
	config *config.Config
	ft     *filetree.FileTreeExt
	hub    *hub.Hub
	log    log.Logger
	sync.Mutex
}

// Close cleanly shutdown thes AppsManager
func (apps *Apps) Close() error {
	return nil
}

func (app *App) visit(path string, f os.FileInfo, err error) error {
	p, err := filepath.Rel(app.path, path)
	if err != nil {
		return err
	}
	app.index[p] = f
	return nil
}

func (app *App) reload() error {
	if app.path == "" {
		return nil
	}
	app.index = map[string]os.FileInfo{}
	if err := filepath.Walk(app.path, app.visit); err != nil {
		return err
	}
	return nil
}

func (app *App) watch() {
	if app.path == "" {
		return
	}
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
				// if event.Op&fsnotify.Write == fsnotify.Write {
				// log.Println("modified file:", event.Name)
				if err := app.reload(); err != nil {
					panic(err)
				}
			case err := <-watcher.Errors:
				panic(err)
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

// App handle an app meta data
type App struct {
	path, name string
	entrypoint string
	domain     string
	config     map[string]interface{}
	auth       func(*http.Request) bool

	proxyTarget *url.URL
	proxy       *rhttputil.ReverseProxy

	index map[string]os.FileInfo
	log   log.Logger
	mu    sync.Mutex
}

func (apps *Apps) newApp(appConf *config.AppConfig) (*App, error) {
	app := &App{
		path:       appConf.Path,
		name:       appConf.Name,
		domain:     appConf.Domain,
		entrypoint: appConf.Entrypoint,
		config:     appConf.Config,
		index:      map[string]os.FileInfo{},
		log:        apps.log.New("app", appConf.Name),
		mu:         sync.Mutex{},
	}

	if appConf.Username != "" || appConf.Password != "" {
		app.auth = httputil.BasicAuthFunc(appConf.Username, appConf.Password)
	}

	if appConf.Proxy != "" {
		// XXX(tsileo): only allow domain for proxy?
		url, err := url.Parse(appConf.Proxy)
		if err != nil {
			return nil, fmt.Errorf("failed to parse proxy URL target: %v", err)
		}
		app.proxy = rhttputil.NewSingleHostReverseProxy(url)
		app.log.Info("proxy registered", "url", url)
	}

	// TODO(tsileo): check that `path` exists, create it if it doesn't exist?
	app.log.Debug("new app")
	return app, app.reload()
}

func (apps *Apps) appUpdateCallback(ctx context.Context, _ *blob.Blob, data interface{}) error {
	// appUpdate := data.(*hub.AppUpdateData)
	// FIXME(tsileo): update the configuration
	return nil
}

// Serve the request for the given path
func (app *App) serve(ctx context.Context, p string, w http.ResponseWriter, req *http.Request) {
	if app.auth != nil {
		if !app.auth(req) {
			w.Header().Set("WWW-Authenticate", fmt.Sprintf("Basic realm=\"BlobStash App %s\"", app.name))
			w.WriteHeader(http.StatusUnauthorized)
		}
		return
	}

	// Clean the path and check there's no double dot
	p = path.Clean(p)
	if containsDotDot(p) {
		w.WriteHeader(500)
		w.Write([]byte("Invalid URL path"))
	}

	app.log.Info("Serving", "app", app)
	if app.proxy != nil {
		app.log.Info("Proxying request", "path", p)
		req.URL.Path = p
		app.proxy.ServeHTTP(w, req)
		return
	}

	// Determine the default entry point if the path is the root
	if p == "/" {
		if app.entrypoint != "" {
			if !strings.HasPrefix(app.entrypoint, "/") {
				w.WriteHeader(500)
				w.Write([]byte("Invalid app entrypoint, must start with a /"))
				return
			}
			p = app.entrypoint
		} else {
			// XXX(tsileo): find a way to do directory listing?
			p = "/index.html"
		}
	}

	// FIXME(tsileo): enable this if config.AppMode == true
	// f, err := os.Open(filepath.Join(app.path, "app.lua"))
	// defer f.Close()
	// if err != nil {
	// 	panic(err)
	// }
	// script, err := ioutil.ReadAll(f)
	// if err != nil {
	// 	panic(err)
	// }
	// app.doLua(string(script), req, w)
	// return

	// Inspect the file
	app.log.Info("serve", "path", p)
	if fi, ok := app.index[p[1:]]; ok {
		// Open the file
		f, err := os.Open(filepath.Join(app.path, p))
		defer f.Close()
		if err != nil {
			panic(err)
		}

		// The node is a Lua script, execute it
		if strings.HasSuffix(p, ".lua") {
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
			if p != "index.html" {
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
	handle404(w)
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
	L.SetGlobal("md5", L.NewFunction(func(L *lua.LState) int {
		L.Push(lua.LString(fmt.Sprintf("%x", md5.Sum([]byte(L.ToString(1))))))
		return 1
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
	app.log.Info("script executed", "time", time.Since(start), "script", script)
	return nil
}

// New initializes the Apps manager
func New(logger log.Logger, conf *config.Config, ft *filetree.FileTreeExt, chub *hub.Hub) (*Apps, error) {
	// var err error
	apps := &Apps{
		apps:   map[string]*App{},
		ft:     ft,
		log:    logger,
		config: conf,
		hub:    chub,
	}
	chub.Subscribe(hub.ScanBlob, "apps", apps.appUpdateCallback)
	for _, appConf := range conf.Apps {
		app, err := apps.newApp(appConf)
		if err != nil {
			return nil, err
		}
		fmt.Printf("app %+v\n", app)
		apps.apps[app.name] = app
		// Watch the app directory for re-scanning it when necessary
		go app.watch()
	}
	return apps, nil
}

func handle404(w http.ResponseWriter) {
	w.WriteHeader(http.StatusNotFound)
	w.Write([]byte(http.StatusText(http.StatusNotFound)))
}

func (apps *Apps) appHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	// First, find which app we're trying to call
	appName := vars["name"]
	// => select the app and call its handler?
	app, ok := apps.apps[appName]
	if !ok {
		apps.log.Warn("unknown app called", "app", appName)
		handle404(w)
		return
	}
	p := vars["path"]
	req.URL.Path = "/" + p
	app.serve(context.TODO(), "/"+p, w, req)
}

func (apps *Apps) subdomainHandler(app *App) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		apps.log.Info("subdomain handler")
		app.serve(context.TODO(), r.URL.Path, w, r)
	}
}

// Register Apps endpoint
func (apps *Apps) Register(r *mux.Router, root *mux.Router, basicAuth func(http.Handler) http.Handler) {
	r.Handle("/{name}/", http.HandlerFunc(apps.appHandler))
	r.Handle("/{name}/{path:.+}", http.HandlerFunc(apps.appHandler))
	for _, app := range apps.apps {
		if app.domain != "" {
			apps.log.Info("Registering app", "subdomain", app.domain)
			root.Host(app.domain).HandlerFunc(apps.subdomainHandler(app))
		}
	}
}

// borrowed from net/http
func containsDotDot(v string) bool {
	if !strings.Contains(v, "..") {
		return false
	}
	for _, ent := range strings.FieldsFunc(v, isSlashRune) {
		if ent == ".." {
			return true
		}
	}
	return false
}

func isSlashRune(r rune) bool { return r == '/' || r == '\\' }
