package apps // import "a4.io/blobstash/pkg/apps"

import (
	"context"
	"fmt"
	"net/http"
	rhttputil "net/http/httputil"
	"net/url"
	"path"
	"strings"
	"sync"

	"github.com/gorilla/mux"
	log "github.com/inconshreveable/log15"
	"github.com/yuin/gopher-lua"

	"a4.io/blobstash/pkg/blob"
	"a4.io/blobstash/pkg/blobstore"
	blobstoreLua "a4.io/blobstash/pkg/blobstore/lua"
	"a4.io/blobstash/pkg/config"
	"a4.io/blobstash/pkg/docstore"
	docstoreLua "a4.io/blobstash/pkg/docstore/lua"
	"a4.io/blobstash/pkg/extra"
	"a4.io/blobstash/pkg/filetree"
	filetreeLua "a4.io/blobstash/pkg/filetree/lua"
	"a4.io/blobstash/pkg/gitserver"
	gitserverLua "a4.io/blobstash/pkg/gitserver/lua"
	"a4.io/blobstash/pkg/httputil"
	"a4.io/blobstash/pkg/hub"
	kvLua "a4.io/blobstash/pkg/kvstore/lua"
	"a4.io/blobstash/pkg/stash/store"
	"a4.io/gluapp"
)

// TODO(tsileo): at startup, scan all filetree FS and looks for app.yaml for registering

// Apps holds the Apps manager data
type Apps struct {
	apps            map[string]*App
	config          *config.Config
	gs              *gitserver.GitServer
	ft              *filetree.FileTree
	bs              *blobstore.BlobStore
	docstore        *docstore.DocStore
	kvs             store.KvStore
	hub             *hub.Hub
	hostWhitelister func(...string)
	log             log.Logger
	sync.Mutex
}

// Close cleanly shutdown thes AppsManager
func (apps *Apps) Close() error {
	return nil
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

	docstore *docstore.DocStore
	app      *gluapp.App

	log log.Logger
	mu  sync.Mutex
}

func (apps *Apps) newApp(appConf *config.AppConfig) (*App, error) {
	app := &App{
		docstore:   apps.docstore,
		path:       appConf.Path,
		name:       appConf.Name,
		domain:     appConf.Domain,
		entrypoint: appConf.Entrypoint,
		config:     appConf.Config,
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
		app.app, err = gluapp.NewApp(&gluapp.Config{
			Path:       app.path,
			Entrypoint: app.entrypoint,
			SetupState: func(L *lua.LState) error {
				docstore.SetLuaGlobals(L)
				blobstoreLua.Setup(context.TODO(), L, apps.bs)
				filetreeLua.Setup(L, apps.ft, apps.bs)
				docstoreLua.Setup(L, apps.docstore)
				kvLua.Setup(L, apps.kvs, context.TODO())
				gitserverLua.Setup(L, apps.gs)
				extra.Setup(L)
				return nil
			},
		})
		if err != nil {
			return nil, err
		}
	}

	// TODO(tsileo): check that `path` exists, create it if it doesn't exist?
	app.log.Debug("new app")
	return app, nil
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
func New(logger log.Logger, conf *config.Config, bs *blobstore.BlobStore, kvs store.KvStore, ft *filetree.FileTree, ds *docstore.DocStore, gs *gitserver.GitServer, chub *hub.Hub, hostWhitelister func(...string)) (*Apps, error) {
	// var err error
	apps := &Apps{
		apps:            map[string]*App{},
		ft:              ft,
		log:             logger,
		gs:              gs,
		bs:              bs,
		config:          conf,
		kvs:             kvs,
		hub:             chub,
		docstore:        ds,
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
