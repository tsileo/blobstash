package apps // import "a4.io/blobstash/pkg/apps"

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	rhttputil "net/http/httputil"
	"net/url"
	"os"
	"path"
	"strings"
	"sync"

	humanize "github.com/dustin/go-humanize"
	"github.com/gorilla/mux"
	"github.com/gorilla/sessions"
	log "github.com/inconshreveable/log15"
	"github.com/yuin/gopher-lua"
	git "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"

	"a4.io/blobstash/pkg/apps/luautil"
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
	"a4.io/go/indieauth"
	"github.com/hashicorp/golang-lru"
	"github.com/robfig/cron"
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
	cron            *cron.Cron
	cookieStore     *sessions.CookieStore
	sync.Mutex
}

// Close cleanly shutdown thes AppsManager
func (apps *Apps) Close() error {
	apps.cron.Stop()
	for _, app := range apps.apps {
		if app.tmp != "" {
			if err := os.RemoveAll(app.tmp); err != nil {
				return err
			}
		}
	}
	return nil
}

func (apps *Apps) Apps() map[string]*App {
	return apps.apps
}

// App handle an app meta data
type App struct {
	path, name string
	entrypoint string
	domain     string
	remote     string
	config     map[string]interface{}
	scheduled  string
	auth       func(*http.Request) bool
	ia         *indieauth.IndieAuth

	proxyTarget *url.URL
	proxy       *rhttputil.ReverseProxy

	appCache *lru.Cache

	docstore *docstore.DocStore
	app      *gluapp.App
	repo     *git.Repository
	tree     *object.Tree
	tmp      string

	log log.Logger
	mu  sync.Mutex
}

func (apps *Apps) newApp(appConf *config.AppConfig) (*App, error) {
	appCache, err := lru.New(512)
	if err != nil {
		return nil, err
	}
	app := &App{
		docstore:   apps.docstore,
		path:       appConf.Path,
		name:       appConf.Name,
		domain:     appConf.Domain,
		remote:     appConf.Remote,
		entrypoint: appConf.Entrypoint,
		config:     appConf.Config,
		appCache:   appCache,
		scheduled:  appConf.Scheduled,
		log:        apps.log.New("app", appConf.Name),
		mu:         sync.Mutex{},
	}

	if appConf.Username != "" || appConf.Password != "" {
		app.auth = httputil.BasicAuthFunc(appConf.Username, appConf.Password)
	}
	if appConf.IndieAuthEndpoint != "" {
		ia, err := indieauth.New(apps.cookieStore, appConf.IndieAuthEndpoint)
		if err != nil {
			return nil, err
		}
		ia.RedirectPath = "/api/apps/" + app.name + "/indieauth-redirect"
		app.auth = ia.Check
		app.ia = ia
	}

	// If it's a remote app, clone the repo in a temp dir
	if appConf.Remote != "" {
		// Format of the remote is `<repo_url>#<commit_hash>`
		parts := strings.Split(appConf.Remote, "#")
		dir, err := ioutil.TempDir("", fmt.Sprintf("blobstash-app-%s-", app.name))
		if err != nil {
			return nil, err
		}

		// the temp dir will be removed at shutdown
		app.tmp = dir

		// Actually do the git clone
		r, err := git.PlainClone(app.tmp, false, &git.CloneOptions{
			URL: parts[0],
		})
		if err != nil {
			return nil, err
		}

		// Checkout the pinned hash
		wt, err := r.Worktree()
		if err != nil {
			return nil, err
		}
		app.repo = r
		coOpts := &git.CheckoutOptions{
			Hash: plumbing.NewHash(parts[1]),
		}
		if err := wt.Checkout(coOpts); err != nil {
			return nil, err
		}
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

	if app.scheduled != "" {
		apps.cron.AddFunc(app.scheduled, func() {
			app.log.Info("running the (scheduled) app")
			// TODO(tsileo): add LuaHook instead of gluapp with
			// app.config, app.log, what for input payload?
		})
		// Return now
		app.log.Debug("new app")
		return app, nil
	}

	// Setup the gluapp app
	if app.path != "" {
		var err error
		app.app, err = gluapp.NewApp(&gluapp.Config{
			Path:       app.path,
			Entrypoint: app.entrypoint,
			SetupState: func(L *lua.LState) error {
				cache := app.buildCache(L)

				confTable := L.NewTable()
				// Set the `app` global variable
				fmt.Printf("app=%+v\n", app)
				confTable.RawSetString("app_id", lua.LString(app.name))
				confTable.RawSetString("cache", cache)
				L.SetGlobal("blobstash", confTable)

				docstore.SetLuaGlobals(L)
				blobstoreLua.Setup(context.TODO(), L, apps.bs)
				filetreeLua.Setup(L, apps.ft, apps.bs, apps.kvs)
				docstoreLua.Setup(L, apps.docstore)
				kvLua.Setup(L, apps.kvs, context.TODO())
				gitserverLua.Setup(L, apps.gs)
				// setup "apps"
				setup(L, apps)
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

func (app *App) buildCache(L *lua.LState) *lua.LTable {
	confTable := L.NewTable()
	mt := L.NewTypeMetatable("blobstash_cache")
	L.SetField(mt, "__index", L.NewFunction(func(ls *lua.LState) int {
		cached, ok := app.appCache.Get(ls.Get(2))
		if !ok {
			ls.Push(lua.LNil)
		} else {
			ls.Push(cached.(lua.LValue))
		}
		return 1
	}))
	L.SetField(mt, "__newindex", L.NewFunction(func(ls *lua.LState) int {
		// FIXME(tsileo): extract the LGFunction for functions and reject invalid types
		app.appCache.Add(ls.Get(2), ls.Get(3))
		return 0
	}))

	L.SetMetatable(confTable, L.GetTypeMetatable("blobstash_cache"))
	return confTable
}

// Serve the request for the given path
func (app *App) serve(ctx context.Context, p string, w http.ResponseWriter, req *http.Request) {
	if app.auth != nil {
		if !app.auth(req) {
			// Handle IndieAuth
			if app.ia != nil {
				if err := app.ia.Redirect(w, req); err != nil {
					if err == indieauth.ErrForbidden {
						w.WriteHeader(http.StatusForbidden)
						return
					}
					panic(err)
				}
			} else {
				// Basic auth
				w.Header().Set("WWW-Authenticate", fmt.Sprintf("Basic realm=\"BlobStash App %s\"", app.name))
				w.WriteHeader(http.StatusUnauthorized)
			}
			return
		}
	}

	// Fake the Path for the App
	req.URL.Path = p

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
	if conf.SecretKey == "" {
		return nil, fmt.Errorf("missing secret_key in config")
	}
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
		cookieStore:     sessions.NewCookieStore([]byte(conf.SecretKey)),
		cron:            cron.New(),
		hostWhitelister: hostWhitelister,
	}
	apps.cron.Start()
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
	// No auth yet, handle the IndieAuth redirect flow
	if p == "indieauth-redirect" && app.ia != nil {
		app.ia.RedirectHandler(w, req)
		return
	}
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

func setupApps(apps *Apps) func(*lua.LState) int {
	return func(L *lua.LState) int {
		// register functions to the table
		mod := L.SetFuncs(L.NewTable(), map[string]lua.LGFunction{
			"apps": func(L *lua.LState) int {
				t := L.NewTable()
				for name, app := range apps.Apps() {
					fmt.Printf("app=%+v\n", app)
					tapp := L.NewTable()
					tapp.RawSetH(lua.LString("name"), lua.LString(name))
					tapp.RawSetH(lua.LString("domain"), lua.LString(app.domain))
					tapp.RawSetH(lua.LString("entrypoint"), lua.LString(app.entrypoint))
					tapp.RawSetH(lua.LString("remote"), lua.LString(app.remote))
					t.Append(tapp)
				}
				L.Push(t)
				return 1
			},
		})
		// returns the module
		L.Push(mod)
		return 1
	}
}

func setup(L *lua.LState, apps *Apps) {
	//mtCol := L.NewTypeMetatable("col")
	//L.SetField(mtCol, "__index", L.SetFuncs(L.NewTable(), map[string]lua.LGFunction{
	//	"insert": colInsert,
	//	"query":  colQuery,
	//}))
	L.PreloadModule("_blobstash", func(L *lua.LState) int {
		// register functions to the table
		mod := L.SetFuncs(L.NewTable(), map[string]lua.LGFunction{
			"status": func(L *lua.LState) int {
				stats, err := apps.bs.S3Stats()
				if err != nil {
					if err != blobstore.ErrRemoteNotAvailable {
						panic(err)
					}
				}
				bstats, err := apps.bs.Stats()
				if err != nil {
					panic(err)
				}
				lbstats := L.CreateTable(0, 4)
				lbstats.RawSetString("blobs_count", lua.LNumber(bstats.BlobsCount))
				lbstats.RawSetString("blobs_size", lua.LNumber(bstats.BlobsSize))
				lbstats.RawSetString("blobs_size_human", lua.LString(humanize.Bytes(uint64(bstats.BlobsSize))))
				lbstats.RawSetString("blobs_blobsfile_volumes", lua.LNumber(bstats.BlobsFilesCount))

				out := L.CreateTable(0, 2)
				out.RawSetString("blobstore", lbstats)
				out.RawSetString("s3", luautil.InterfaceToLValue(L, stats))

				L.Push(out)
				return 1
			},
		})
		L.Push(mod)
		return 1
	})
	L.PreloadModule("apps", setupApps(apps))
}
