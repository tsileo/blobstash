package lua

import (
	"bytes"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/cjoudrey/gluahttp"
	"github.com/dchest/blake2b"
	"github.com/gorilla/mux"
	luajson "github.com/layeh/gopher-json"
	"github.com/russross/blackfriday"
	"github.com/tsileo/blobstash/bewit"
	"github.com/tsileo/blobstash/client/interface"
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
)

// do return end
const test = `
local log = require('logger')
local resp = require('response')
local req = require('request')
local json= require('json')
local bewit = require('bewit')

log.info(string.format("it works, bewit=%s", bewit.new("http://localhost:8050/api/ext/lua/v1/")))
log.info(string.format("bewit check=%q", bewit.check()))
log.info(string.format('ok=%s', req.queryarg('ok')))
local tpl = [[<html>
<head><title>BlobStash</title></head>
<body><p>Nothing to see here Mr {{ .name }}</p></body>
</html>]]
resp.write(render(tpl, json.encode({name = 'Thomas'})))
resp.header('My-Header', 'value')
resp.status(404)
log.info(string.format("body=%s\nmethod=%s", json.decode(req.body()), req.method()))
`

// TODO App ACL (public/private)?
// Store recent log entries

type LuaApp struct {
	AppID  string
	Name   string
	Script []byte
	Hash   string
	Public bool
	Stats  *LuaAppStats
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
	bewit.SetKey(key)
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
	r.Handle("/stats", middlewares.Auth(http.HandlerFunc(lua.AppStatHandler())))
	r.Handle("/register", middlewares.Auth(http.HandlerFunc(lua.RegisterHandler())))
	// TODO(tsileo) "/remove" endpoint
}

func (lua *LuaExt) RegisterAppRoute(r *mux.Router, middlewares *serverMiddleware.SharedMiddleware) {
	r.HandleFunc("/{appID}", lua.AppHandler())
}

func (lua *LuaExt) RegisterApp(app *LuaApp) error {
	// lua.registeredApps[
	return nil
}

func setCustomGlobals(L *luamod.LState) {
	// Return the server unix timestamp
	L.SetGlobal("unix", L.NewFunction(func(L *luamod.LState) int {
		L.Push(luamod.LNumber(time.Now().Unix()))
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

	// Render execute a Go template, data must be encoded as JSON
	L.SetGlobal("render", L.NewFunction(func(L *luamod.LState) int {
		// TODO(tsileo) turn this into a module with a data attribute that map to a map[string]interface{}
		tplString := L.ToString(1)
		data := map[string]interface{}{}
		// XXX(tsileo) find a better way to handle the data than a JSON encoding/decoding
		// to share data with Lua
		if err := json.Unmarshal([]byte(L.ToString(2)), &data); err != nil {
			L.Push(luamod.LString(err.Error()))
			return 1
		}
		tpl, err := template.New("tpl").Parse(tplString)
		if err != nil {
			L.Push(luamod.LString(err.Error()))
			return 1
		}
		out := &bytes.Buffer{}
		if err := tpl.Execute(out, data); err != nil {
			L.Push(luamod.LString(err.Error()))
			return 1
		}
		L.Push(luamod.LString(out.String()))
		return 1
	}))

	// Return an absoulte URL for the given path
	L.SetGlobal("url", L.NewFunction(func(L *luamod.LState) int {
		// FIXME(tsileo) take the host from the req?
		L.Push(luamod.LString("http://localhost:8050" + L.ToString(1)))
		return 1
	}))
}

// FIXME(ts) move this in utils/http
func WriteJSON(w http.ResponseWriter, data interface{}) {
	js, err := json.Marshal(data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(js)
}

func appToResp(app *LuaApp) *LuaAppResp {
	return &LuaAppResp{
		AppID:               app.AppID,
		Name:                app.Name,
		Hash:                app.Hash,
		Public:              app.Public,
		Requests:            app.Stats.Requests,
		Statuses:            app.Stats.Statuses,
		StartedAt:           app.Stats.StartedAt,
		AverageResponseTime: time.Duration(float64(app.Stats.TotalTime) / float64(app.Stats.Requests)).String(),
	}

}

func (lua *LuaExt) AppStatHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		lua.appMutex.Lock()
		defer lua.appMutex.Unlock()
		app, ok := lua.registeredApps[r.URL.Query().Get("appID")]
		if !ok {
			panic("no such app")
		}
		WriteJSON(w, appToResp(app))
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
		WriteJSON(w, map[string]interface{}{
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
			//parse the multipart form in the request
			mr, err := r.MultipartReader()
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			for {
				part, err := mr.NextPart()
				if err == io.EOF {
					break
				}
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
					Stats:  NewAppStats(),
				}
				lua.appMutex.Lock()
				lua.registeredApps[appID] = app
				lua.appMutex.Unlock()
				lua.logger.Info("Registered new app", "appID", appID, "app", app)
			}

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
		app, ok := lua.registeredApps[appID]
		if !ok {
			lua.appMutex.Unlock()
			panic("no such app")
		}
		if r.Method == "HEAD" {
			w.Header().Add("BlobStash-App-Script-Hash", app.Hash)
			lua.appMutex.Unlock()
			return
		}
		if !app.Public && !lua.authFunc(r) {
			lua.appMutex.Unlock()
			w.Header().Set("WWW-Authenticate", "Basic realm=\""+app.AppID+"\"")
			http.Error(w, "Not Authorized", http.StatusUnauthorized)
			return
		}
		script := make([]byte, len(app.Script))
		copy(script[:], app.Script[:])
		lua.appMutex.Unlock()

		// Execute the script
		lua.logger.Info("Starting", "appID", appID)
		start := time.Now()
		status := strconv.Itoa(lua.exec(string(script), w, r))

		// Increment the internal stats
		lua.appMutex.Lock()
		app, ok = lua.registeredApps[appID]
		defer lua.appMutex.Unlock()
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
	}
}

func (lua *LuaExt) ScriptHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		lua.exec(test, w, r)
	}
}

func (lua *LuaExt) exec(script string, w http.ResponseWriter, r *http.Request) int {
	start := time.Now()
	httpClient := &http.Client{}
	reqId := logext.RandId(8)
	reqLogger := lua.logger.New("id", reqId)
	reqLogger.Debug("Starting script execution")
	// Initialize internal Lua module written in Go
	logger := loggerModule.New(reqLogger.New("ctx", "inside script"), start)
	response := responseModule.New()
	request := requestModule.New(r, reqId, lua.authFunc)
	blobstore := blobstoreModule.New(lua.blobStore)
	kvstore := kvstoreModule.New(lua.kvStore)
	bewit := bewitModule.New(reqLogger.New("ctx", "bewit module"), r)

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

	// 3rd party module
	luajson.Preload(L)
	L.PreloadModule("http", gluahttp.NewHttpModule(httpClient).Loader)

	// Execute the code
	if err := L.DoString(script); err != nil {
		// FIXME better error, with debug mode?
		panic(err)
	}

	// Apply the Response object to the actual response
	response.WriteTo(w)

	reqLogger.Info("Script executed", "response", response, "duration", time.Since(start))
	return response.Status()
}
