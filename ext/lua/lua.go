package lua

import (
	"bytes"
	"fmt"
	"html/template"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cjoudrey/gluahttp"
	"github.com/dchest/blake2b"
	"github.com/dustin/go-humanize"
	"github.com/gorilla/mux"
	"github.com/hashicorp/golang-lru"
	luajson "github.com/layeh/gopher-json"
	"github.com/russross/blackfriday"
	"github.com/satori/go.uuid"
	luamod "github.com/yuin/gopher-lua"
	log "gopkg.in/inconshreveable/log15.v2"
	logext "gopkg.in/inconshreveable/log15.v2/ext"

	"github.com/tsileo/blobstash/embed"
	"github.com/tsileo/blobstash/ext/docstore"
	hexid "github.com/tsileo/blobstash/ext/docstore/id"
	"github.com/tsileo/blobstash/ext/lua/luautil"
	"github.com/tsileo/blobstash/httputil"
	serverMiddleware "github.com/tsileo/blobstash/middleware"

	bewitModule "github.com/tsileo/blobstash/ext/lua/modules/bewit"
	blobstoreModule "github.com/tsileo/blobstash/ext/lua/modules/blobstore"
	docstoreModule "github.com/tsileo/blobstash/ext/lua/modules/docstore"
	filetreeModule "github.com/tsileo/blobstash/ext/lua/modules/filetree"
	kvstoreModule "github.com/tsileo/blobstash/ext/lua/modules/kvstore"
	loggerModule "github.com/tsileo/blobstash/ext/lua/modules/logger"
	lruModule "github.com/tsileo/blobstash/ext/lua/modules/lru"
	requestModule "github.com/tsileo/blobstash/ext/lua/modules/request"
	responseModule "github.com/tsileo/blobstash/ext/lua/modules/response"
	templateModule "github.com/tsileo/blobstash/ext/lua/modules/template"
)

// TODO(tsileo): A Lua flag for automatically load/apply Luap app (and ns/meta blobs)
// TODO(tsileo): Store recent log entries
// TODO(tsileo): Remove name from app?
// TODO(tsileo): check the authentication per app LuaApp.APIKey

type LuaApp struct {
	AppID  string
	Public bool
	InMem  bool // Don't store the script if true
	Stats  *LuaAppStats
	logs   []*loggerModule.LogRecord
	APIKey string

	Dir       map[string]*LuaAppEntry
	Templates map[string]*template.Template

	lru *lru.Cache // LRU cache accessible from the Lua app
}

const (
	LuaScript  string = "lua_script"
	StaticFile string = "static_file"
)

type LuaAppEntry struct {
	app  *LuaApp
	Name string
	Type string
	Hash string
	Data []byte
}

func (lae *LuaAppEntry) Serve(lua *LuaExt, reqLogger log.Logger, reqID string, r *http.Request, w http.ResponseWriter) int {
	reqLogger.Debug("Serving LuaAppEntry", "type", string(lae.Type))
	switch lae.Type {
	case LuaScript:
		return lua.exec(reqLogger, lae, reqID, string(lae.Data), w, r)
	case StaticFile:
		w.Header().Set("Content-Type", mime.TypeByExtension(lae.Name))
		w.Write(lae.Data)
		return 200
	}
	panic("unknow entry type")
}

// FIXME(tsileo) generate an UUID v4 as API key and assign an API key for each app

func (app *LuaApp) String() string {
	return fmt.Sprintf("[appID=%v, public=%v, inMem=%v]",
		app.AppID, app.Public, app.InMem)
}

type LuaAppStats struct {
	Requests  int
	Statuses  map[string]int
	StartedAt string
	TotalTime time.Duration
}

type LuaAppResp struct {
	AppID               string         `json:"app_id"`
	Name                string         `json:"name"`
	Public              bool           `json:"is_public"`
	InMem               bool           `json:"is_script_in_memory"`
	Hash                string         `json:"script_hash"`
	Requests            int            `json:"stats_requests"`
	StartedAt           string         `json:"stats_started_at"`
	AverageResponseTime string         `json:"stats_avg_response_time"`
	Statuses            map[string]int `json:"stats_statuses"`
}

func NewAppStats() *LuaAppStats {
	return &LuaAppStats{
		Requests:  0,
		Statuses:  map[string]int{},
		StartedAt: time.Now().UTC().Format(time.RFC3339),
		TotalTime: time.Duration(0),
	}
}

type LuaExt struct {
	conf   map[string]interface{}
	logger log.Logger

	// Key for the Hawk bewit auth
	hawkKey []byte

	authFunc func(*http.Request) bool

	docstore  *docstore.DocStoreExt
	kvStore   *embed.KvStore
	blobStore *embed.BlobStore

	appMutex       sync.Mutex
	registeredApps map[string]*LuaApp
}

func New(conf map[string]interface{}, logger log.Logger, key []byte, authFunc func(*http.Request) bool, kvStore *embed.KvStore,
	blobStore *embed.BlobStore, docstore *docstore.DocStoreExt) *LuaExt {
	httputil.SetHawkKey(key)
	return &LuaExt{
		conf:           conf,
		hawkKey:        key,
		logger:         logger,
		kvStore:        kvStore,
		blobStore:      blobStore,
		docstore:       docstore,
		registeredApps: map[string]*LuaApp{},
		authFunc:       authFunc,
	}
}

func (lua *LuaExt) RegisterRoute(r *mux.Router, middlewares *serverMiddleware.SharedMiddleware) {
	r.Handle("/", middlewares.Auth(http.HandlerFunc(lua.AppsHandler())))
	r.Handle("/stats", middlewares.Auth(http.HandlerFunc(lua.AppStatsHandler())))
	r.Handle("/logs", middlewares.Auth(http.HandlerFunc(lua.AppLogsHandler())))
	r.Handle("/register", middlewares.Auth(http.HandlerFunc(lua.RegisterHandler())))
	// TODO(tsileo) "/remove" endpoint
	// TODO(tsileo) "/logstream" endpoint to stream logs (SSE)
}

// FIXME(tsileo) 404 on no such app error and log panic as crit level in the logger

func (lua *LuaExt) RegisterAppRoute(r *mux.Router, middlewares *serverMiddleware.SharedMiddleware) {
	appHandler := lua.AppHandler()
	r.HandleFunc("/{appID}", appHandler)
	r.HandleFunc("/{appID}/", appHandler)
	r.HandleFunc("/{appID}/{path:.+}", appHandler)
	// FIXME(tsileo) a way to hook an app to / (root)
}

func setCustomGlobals(L *luamod.LState, lua *LuaExt, app *LuaApp) {
	// Return the server unix timestamp
	L.SetGlobal("unix", L.NewFunction(func(L *luamod.LState) int {
		L.Push(luamod.LNumber(time.Now().Unix()))
		return 1
	}))

	// Generate a random hexadecimal ID with the current timestamp as first 4 bytes,
	// this means keys will be sorted by creation date automatically if sorted lexicographically
	L.SetGlobal("hexid", L.NewFunction(func(L *luamod.LState) int {
		id, err := hexid.New(int(time.Now().UTC().Unix()))
		if err != nil {
			panic(err)
		}
		L.Push(luamod.LString(id.String()))
		return 1
	}))

	// Compute the Blake2B hash for the given string
	L.SetGlobal("blake2b", L.NewFunction(func(L *luamod.LState) int {
		hash := fmt.Sprintf("%x", blake2b.Sum256([]byte(L.ToString(1))))
		L.Push(luamod.LString(hash))
		return 1
	}))

	// Sleep for the given number of seconds
	L.SetGlobal("sleep", L.NewFunction(func(L *luamod.LState) int {
		time.Sleep(time.Duration(float64(L.ToNumber(1)) * float64(1e9)))
		return 0
	}))

	// Convert the given Markdown to HTML
	L.SetGlobal("markdownify", L.NewFunction(func(L *luamod.LState) int {
		output := blackfriday.MarkdownCommon([]byte(L.ToString(1)))
		L.Push(luamod.LString(string(output)))
		return 1
	}))

	// Render execute a Go HTML template, data must be a table with string keys
	// FIXME(tsileo): use text/template for this one
	L.SetGlobal("render", L.NewFunction(func(L *luamod.LState) int {
		tplString := L.ToString(1)
		data := luautil.TableToMap(L.ToTable(2))
		tpl, err := template.New("tpl").Parse(tplString)
		if err != nil {
			L.Push(luamod.LString(err.Error()))
			return 1
		}
		// TODO(tsileo) add some templatFuncs/template filter
		out := &bytes.Buffer{}
		if err := tpl.Execute(out, data); err != nil {
			L.Push(luamod.LString(err.Error()))
			return 1
		}
		L.Push(luamod.LString(out.String()))
		return 1
	}))

	// TODO(tsileo) a urljoin?
	hostname := "http://localhost:8050"
	if tlsHost, ok := lua.conf["tls-hostname"]; ok {
		hostname = "https://" + tlsHost.(string)
	}

	// Return an absoulte URL for the given path
	L.SetGlobal("path", L.NewFunction(func(L *luamod.LState) int {
		// FIXME(tsileo) take the host from the req?
		L.Push(luamod.LString(hostname + "/app/" + app.AppID + L.ToString(1)))
		return 1
	}))
}

func appToResp(app *LuaApp) *LuaAppResp {
	return &LuaAppResp{
		AppID:               app.AppID,
		Public:              app.Public,
		InMem:               app.InMem,
		Requests:            app.Stats.Requests,
		Statuses:            app.Stats.Statuses,
		StartedAt:           app.Stats.StartedAt,
		AverageResponseTime: time.Duration(float64(app.Stats.TotalTime) / float64(app.Stats.Requests)).String(),
	}

}

func (lua *LuaExt) AppLogsHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		lua.appMutex.Lock()
		defer lua.appMutex.Unlock()
		app, ok := lua.registeredApps[r.URL.Query().Get("appID")]
		if !ok {
			panic("no such app")
		}
		httputil.WriteJSON(w, map[string]interface{}{
			"logs": app.logs,
		})
	}
}

func (lua *LuaExt) AppStatsHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		lua.appMutex.Lock()
		defer lua.appMutex.Unlock()
		app, ok := lua.registeredApps[r.URL.Query().Get("appID")]
		if !ok {
			panic("no such app")
		}
		httputil.WriteJSON(w, appToResp(app))
	}
}

func (lua *LuaExt) AppsHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		lua.appMutex.Lock()
		defer lua.appMutex.Unlock()
		apps := []*LuaAppResp{}
		for _, app := range lua.registeredApps {
			apps = append(apps, appToResp(app))
		}
		httputil.WriteJSON(w, map[string]interface{}{
			"apps": apps,
		})
	}
}

func (lua *LuaExt) RegisterHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		//POST takes the uploaded file(s) and saves it to disk.
		case "POST":
			appID := r.URL.Query().Get("appID")
			if appID == "" {
				panic("Missing \"appID\"")
			}
			public, _ := strconv.ParseBool(r.URL.Query().Get("public"))
			inMem, _ := strconv.ParseBool(r.URL.Query().Get("in_memory"))
			// TODO(tsileo) Handle ACL like "authorized_methods" GET,POST...
			// FIXME Find a better way to setup the environment
			//parse the multipart form in the request
			mr, err := r.MultipartReader()
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			// FIXME(tsileo): LRU size as config
			appLRU, err := lru.New(128)
			if err != nil {
				// TODO(tsileo): use httputil.Error
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			app := &LuaApp{
				Public:    public,
				AppID:     appID,
				Dir:       map[string]*LuaAppEntry{},
				Templates: map[string]*template.Template{},
				InMem:     inMem,
				Stats:     NewAppStats(),
				APIKey:    uuid.NewV4().String(),
				lru:       appLRU,
			}
			for {
				part, err := mr.NextPart()
				if err == io.EOF {
					break
				}
				if err != nil {
					httputil.Error(w, err)
					return
				}
				filename := part.FormName()
				var buf bytes.Buffer
				buf.ReadFrom(part)
				blob := buf.Bytes()
				// FIXME(tsileo): save the blob as a filetree meta if not -in-mem
				chash := fmt.Sprintf("%x", blake2b.Sum256(blob))
				var appEntry *LuaAppEntry
				switch {
				case strings.HasSuffix(filename, ".lua"):
					appEntry = &LuaAppEntry{
						app:  app,
						Name: filename,
						Type: LuaScript,
						Hash: chash,
						Data: blob,
					}
				case strings.HasSuffix(filename, ".tpl"):
					funcMap := template.FuncMap{
						"bytes": func(n luamod.LNumber) string {
							res := humanize.Bytes(uint64(n))
							return res
						},
						"time": func(s luamod.LString) string {
							t, err := time.Parse(time.RFC3339, string(s))
							if err != nil {
								return ""
							}
							return humanize.Time(t)

						},
						"path": func(s string) string {
							return fmt.Sprintf("/app/%s%s", app.AppID, s)
						},
					}
					tpl, err := template.New("tpl").Funcs(funcMap).Parse(string(blob))
					if err != nil {
						panic("bad template")
					}
					app.Templates[filename] = tpl
				default:
					appEntry = &LuaAppEntry{
						app:  app,
						Name: filename,
						Type: StaticFile,
						Hash: chash,
						Data: blob,
					}

				}
				app.Dir[filename] = appEntry
			}

			lua.appMutex.Lock()
			lua.registeredApps[appID] = app
			lua.appMutex.Unlock()
			lua.logger.Info("Registered new app", "appID", appID, "app", app.String())
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}
}

func (lua *LuaExt) AppHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)

		spath := "index.lua"
		if gspath, ok := vars["path"]; ok {
			spath = gspath
		}
		// FIXME(tsileo): handle index.html as root

		// Try to fetch the app
		appID := vars["appID"]
		lua.appMutex.Lock()
		defer lua.appMutex.Unlock()
		app, ok := lua.registeredApps[appID]
		if !ok {
			panic("no such app")
		}

		reqID := logext.RandId(8)
		reqLogger := lua.logger.New("reqID", reqID, "appID", appID)
		reqLogger.Info("Starting", "app", app.String(), "path", "/"+spath)

		w.Header().Add("BlobStash-App-ID", appID)
		w.Header().Add("BlobStash-App-Req-ID", reqID)
		// w.Header().Add("BlobStash-App-Script-Hash", app.Hash)

		// Out the hash script on HEAD request to allow app manager/owner
		// to verify if the script exists, and compare local version
		if r.Method == "HEAD" {
			reqLogger.Debug("HEAD request, aborting...")
			return
		}

		// Check the app ACL
		if !app.Public && !lua.authFunc(r) {
			w.Header().Set("WWW-Authenticate", "Basic realm=\""+app.AppID+"\"")
			http.Error(w, "Not Authorized", http.StatusUnauthorized)
			return
		}

		appEntry, ok := app.Dir[spath]
		if !ok {
			reqLogger.Info("Path not found in app dir", "dir", app.Dir)
			httputil.WriteJSONError(w, http.StatusNotFound, http.StatusText(http.StatusNotFound))
			return
		}

		// Execute the script
		start := time.Now()
		status := strconv.Itoa(appEntry.Serve(lua, reqLogger, reqID, r, w))

		// Increment the internal stats
		app, ok = lua.registeredApps[appID]
		if !ok {
			panic("App seems to have been deleted")
		}
		app.Stats.Requests++
		if _, ok := app.Stats.Statuses[status]; !ok {
			app.Stats.Statuses[status] = 1
		} else {
			app.Stats.Statuses[status]++
		}
		app.Stats.TotalTime += time.Since(start)
		w.Header().Add("BlobStash-App-Script-Execution-Time", time.Since(start).String())
	}
}

func (lua *LuaExt) exec(reqLogger log.Logger, appEntry *LuaAppEntry, reqId, script string, w http.ResponseWriter, r *http.Request) int {
	// FIXME(tsileo) a debug mode, with a defer/recover
	// also parse the Lu error and show the bugging line!
	app := appEntry.app
	start := time.Now()
	httpClient := &http.Client{}
	// Initialize internal Lua module written in Go
	logger := loggerModule.New(reqLogger.New("ctx", "Lua"), start, reqId)
	response := responseModule.New()
	request := requestModule.New(r, reqId, lua.authFunc)
	blobstore := blobstoreModule.New(lua.blobStore)
	kvstore := kvstoreModule.New(lua.kvStore)
	bewit := bewitModule.New(reqLogger.New("ctx", "Lua bewit module"), r)
	template := templateModule.New(app.Templates)
	filetree := filetreeModule.New(lua.blobStore, r, w)
	docstore := docstoreModule.New(lua.docstore)
	lru := lruModule.New(reqLogger.New("ctx", "lru_cache"), app.lru)

	// Initialize Lua state
	L := luamod.NewState()
	defer L.Close()
	setCustomGlobals(L, lua, app)
	L.PreloadModule("request", request.Loader)
	L.PreloadModule("response", response.Loader)
	L.PreloadModule("logger", logger.Loader)
	L.PreloadModule("blobstore", blobstore.Loader)
	L.PreloadModule("kvstore", kvstore.Loader)
	L.PreloadModule("bewit", bewit.Loader)
	L.PreloadModule("template", template.Loader)
	L.PreloadModule("filetree", filetree.Loader)
	L.PreloadModule("docstore", docstore.Loader)
	L.PreloadModule("lru", lru.Loader)
	// TODO(tsileo) docstore module
	// TODO(tsileo) cookies module
	// TODO(tsileo) lru module
	// TODO(tsileo) cache module => to cache response
	// TODO(tsileo) load module from github directly?
	// TODO(tsileo) ETag support

	// 3rd party module
	luajson.Preload(L)
	L.PreloadModule("http", gluahttp.NewHttpModule(httpClient).Loader)

	// Set some global variables
	L.SetGlobal("reqID", luamod.LString(reqId))
	L.SetGlobal("appID", luamod.LString(app.AppID))

	// Execute the code
	if err := L.DoString(script); err != nil {
		// FIXME better error, with debug mode?
		panic(err)
	}

	// Apply the Response object to the actual response
	response.WriteTo(w)
	// TODO save the logRecords in the AppStats and find a way to serve them over Server-Sent Events
	// keep them in memory with the ability to dump them in bulk as blob for later query
	// logRecords := logger.Records()
	for _, logRecord := range logger.Records() {
		app.logs = append(app.logs, logRecord)
	}

	reqLogger.Info("Script executed", "response", response, "duration", time.Since(start))
	return response.Status()
}
