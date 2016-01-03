package lua

import (
	"bytes"
	"fmt"
	"html/template"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/cjoudrey/gluahttp"
	"github.com/dchest/blake2b"
	"github.com/gorilla/mux"
	luajson "github.com/layeh/gopher-json"
	"github.com/russross/blackfriday"
	"github.com/tsileo/blobstash/client/interface"
	hexid "github.com/tsileo/blobstash/ext/docstore/id"
	"github.com/tsileo/blobstash/ext/lua/luautil"
	"github.com/tsileo/blobstash/httputil"
	serverMiddleware "github.com/tsileo/blobstash/middleware"
	luamod "github.com/yuin/gopher-lua"
	log "gopkg.in/inconshreveable/log15.v2"
	logext "gopkg.in/inconshreveable/log15.v2/ext"

	bewitModule "github.com/tsileo/blobstash/ext/lua/modules/bewit"
	blobstoreModule "github.com/tsileo/blobstash/ext/lua/modules/blobstore"
	kvstoreModule "github.com/tsileo/blobstash/ext/lua/modules/kvstore"
	loggerModule "github.com/tsileo/blobstash/ext/lua/modules/logger"
	requestModule "github.com/tsileo/blobstash/ext/lua/modules/request"
	responseModule "github.com/tsileo/blobstash/ext/lua/modules/response"
	templateModule "github.com/tsileo/blobstash/ext/lua/modules/template"
)

// TODO(tsileo) Store recent log entries
// TODO(tsileo) Remove name from app?

type LuaApp struct {
	AppID  string
	Name   string
	Script []byte
	Hash   string
	Public bool
	InMem  bool // Don't store the script if true
	Stats  *LuaAppStats
	logs   []*loggerModule.LogRecord
}

func (app *LuaApp) String() string {
	return fmt.Sprintf("[appID=%v, name=%v, hash=%v, public=%v, inMem=%v]",
		app.AppID, app.Name, app.Hash[:10], app.Public, app.InMem)
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
	logger log.Logger

	// Key for the Hawk bewit auth
	hawkKey []byte

	authFunc func(*http.Request) bool

	kvStore   client.KvStorer
	blobStore client.BlobStorer

	appMutex       sync.Mutex
	registeredApps map[string]*LuaApp
}

func New(logger log.Logger, key []byte, authFunc func(*http.Request) bool, kvStore client.KvStorer, blobStore client.BlobStorer) *LuaExt {
	httputil.SetHawkKey(key)
	return &LuaExt{
		hawkKey:        key,
		logger:         logger,
		kvStore:        kvStore,
		blobStore:      blobStore,
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
	// TODO(tsileo) "/logs" endpoint to stream logs
}

func (lua *LuaExt) RegisterAppRoute(r *mux.Router, middlewares *serverMiddleware.SharedMiddleware) {
	r.HandleFunc("/{appID}", lua.AppHandler())
}

func setCustomGlobals(L *luamod.LState) {
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

	// Return an absoulte URL for the given path
	L.SetGlobal("url", L.NewFunction(func(L *luamod.LState) int {
		// FIXME(tsileo) take the host from the req?
		L.Push(luamod.LString("http://localhost:8050" + L.ToString(1)))
		return 1
	}))
}

func appToResp(app *LuaApp) *LuaAppResp {
	return &LuaAppResp{
		AppID:               app.AppID,
		Name:                app.Name,
		Hash:                app.Hash,
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
			part, err := mr.NextPart()
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			filename := part.FormName()
			var buf bytes.Buffer
			buf.ReadFrom(part)
			blob := buf.Bytes()
			chash := fmt.Sprintf("%x", blake2b.Sum256(blob))
			app := &LuaApp{
				Name:   filename,
				Public: public,
				AppID:  appID,
				Script: blob,
				Hash:   chash,
				InMem:  inMem,
				Stats:  NewAppStats(),
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
		reqLogger.Info("Starting", "app", app.String())

		w.Header().Add("BlobStash-App-ID", appID)
		w.Header().Add("BlobStash-App-Req-ID", reqID)
		w.Header().Add("BlobStash-App-Script-Hash", app.Hash)

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

		// Copy the script so we can release the mutex
		script := make([]byte, len(app.Script))
		copy(script[:], app.Script[:])

		// Execute the script
		start := time.Now()
		status := strconv.Itoa(lua.exec(reqLogger, app, appID, reqID, string(script), w, r))

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

func (lua *LuaExt) exec(reqLogger log.Logger, app *LuaApp, appID, reqId, script string, w http.ResponseWriter, r *http.Request) int {
	// FIXME(tsileo) a debug mode, with a defer/recover
	// also parse the Lu error and show the bugging line!
	start := time.Now()
	httpClient := &http.Client{}
	// Initialize internal Lua module written in Go
	logger := loggerModule.New(reqLogger.New("ctx", "Lua"), start, reqId)
	response := responseModule.New()
	request := requestModule.New(r, reqId, lua.authFunc)
	blobstore := blobstoreModule.New(lua.blobStore)
	kvstore := kvstoreModule.New(lua.kvStore)
	bewit := bewitModule.New(reqLogger.New("ctx", "Lua bewit module"), r)
	template := templateModule.New()

	// Initialize Lua state
	L := luamod.NewState()
	defer L.Close()
	setCustomGlobals(L)
	L.PreloadModule("request", request.Loader)
	L.PreloadModule("response", response.Loader)
	L.PreloadModule("logger", logger.Loader)
	L.PreloadModule("blobstore", blobstore.Loader)
	L.PreloadModule("kvstore", kvstore.Loader)
	L.PreloadModule("bewit", bewit.Loader)
	L.PreloadModule("template", template.Loader)
	// TODO(tsileo) cookies module

	// 3rd party module
	luajson.Preload(L)
	L.PreloadModule("http", gluahttp.NewHttpModule(httpClient).Loader)

	// Set some global variables
	L.SetGlobal("reqID", luamod.LString(reqId))
	L.SetGlobal("appID", luamod.LString(appID))

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
