package apps // import "a4.io/blobstash/pkg/apps"

import (
	"context"
	"fmt"
	"net/http"
	rhttputil "net/http/httputil"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"

	"github.com/fsnotify/fsnotify"
	"github.com/gorilla/mux"
	log "github.com/inconshreveable/log15"

	"a4.io/blobstash/pkg/blob"
	"a4.io/blobstash/pkg/config"
	"a4.io/blobstash/pkg/filetree"
	"a4.io/blobstash/pkg/httputil"
	"a4.io/blobstash/pkg/hub"
	"a4.io/gluapp"
)

// TODO(tsileo): at startup, scan all filetree FS and looks for app.yaml for registering

// Apps holds the Apps manager data
type Apps struct {
	apps            map[string]*App
	config          *config.Config
	ft              *filetree.FileTree
	hub             *hub.Hub
	hostWhitelister func(...string)
	log             log.Logger
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

	app *gluapp.App

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

	if app.path != "" {
		var err error
		app.app, err = gluapp.NewApp(&gluapp.Config{Path: app.path, Entrypoint: app.entrypoint})
		if err != nil {
			return nil, err
		}
	}

	// TODO(tsileo): check that `path` exists, create it if it doesn't exist?
	app.log.Debug("new app")
	return app, app.reload()
}

func (apps *Apps) appUpdateCallback(ctx context.Context, _ *blob.Blob, data interface{}) error {
	// appUpdate := data.(*hub.AppUpdateData)
	// appConfig := &app.AppConfig{}
	// if err := yaml.Unmarshal(appUpdate.RawAppConfig, &appConfig); err != nil {
	// 	return err
	// }
	// appUpdate.Name
	// appUpdate.Ref
	// FIXME(tsileo): update the configuration
	return nil
}

// Serve the request for the given path
func (app *App) serve(ctx context.Context, p string, w http.ResponseWriter, req *http.Request) {
	if app.auth != nil {
		if !app.auth(req) {
			w.Header().Set("WWW-Authenticate", fmt.Sprintf("Basic realm=\"BlobStash App %s\"", app.name))
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
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

	if app.app != nil {
		// FIXME(tsileo): support app not serving from a domain (like blobstashdomain/app/path)
		app.log.Info("Serve gluapp", "path", p)
		app.app.ServeHTTP(w, req)
		return
	}

	handle404(w)
}

// New initializes the Apps manager
func New(logger log.Logger, conf *config.Config, ft *filetree.FileTree, chub *hub.Hub, hostWhitelister func(...string)) (*Apps, error) {
	// var err error
	apps := &Apps{
		apps:            map[string]*App{},
		ft:              ft,
		log:             logger,
		config:          conf,
		hub:             chub,
		hostWhitelister: hostWhitelister,
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
		apps.log.Info("subdomain handler", "app", app)
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
