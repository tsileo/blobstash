package lua

import (
	"fmt"
	"net/http"
	"time"

	"github.com/cjoudrey/gluahttp"
	"github.com/dchest/blake2b"
	"github.com/gorilla/mux"
	luajson "github.com/layeh/gopher-json"
	"github.com/russross/blackfriday"
	luamod "github.com/yuin/gopher-lua"
	log "gopkg.in/inconshreveable/log15.v2"
	logext "gopkg.in/inconshreveable/log15.v2/ext"

	loggerModule "github.com/tsileo/blobstash/ext/lua/modules/logger"
	requestModule "github.com/tsileo/blobstash/ext/lua/modules/request"
	responseModule "github.com/tsileo/blobstash/ext/lua/modules/response"
)

const test = `
local log = require('logger')
local resp = require('response')
local req = require('request')
local json= require('json')

log.info('it works')
resp.write('Nothing to see here!')
resp.header('My-Header', 'value')
resp.status(404)
log.info(string.format("body=%s\nmethod=%s", json.decode(req.body()), req.method()))
`

// log.info(string.format('body=%s', body))
// TODO(tsileo) Load script from filesystem/laoded via HTTP POST
// TODO(tsileo) Find a way to give unique url to script: UUID?
// TODO(tsileo) Hawk Bewit support
// TODO(tsileo) Golang template rendering via html.render

type LuaExt struct {
	logger log.Logger
}

func New(logger log.Logger) *LuaExt {
	return &LuaExt{
		logger: logger,
	}
}

func (lua *LuaExt) RegisterRoute(r *mux.Router) {
	r.HandleFunc("/", lua.ScriptHandler())
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
}

func (lua *LuaExt) ScriptHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		httpClient := &http.Client{}
		reqId := logext.RandId(8)
		reqLogger := lua.logger.New("id", reqId)
		reqLogger.Debug("Starting script execution")

		// Initialize internal Lua module written in Go
		logger := loggerModule.New(reqLogger.New("ctx", "inside script"), start)
		response := responseModule.New()
		request := requestModule.New(r, reqId)

		// Initialize Lua state
		L := luamod.NewState()
		defer L.Close()
		setCustomGlobals(L)
		L.PreloadModule("request", request.Loader)
		L.PreloadModule("response", response.Loader)
		L.PreloadModule("logger", logger.Loader)

		// 3rd party module
		luajson.Preload(L)
		L.PreloadModule("http", gluahttp.NewHttpModule(httpClient).Loader)

		// Execute the code
		if err := L.DoString(test); err != nil {
			// FIXME better error, with debug mode?
			panic(err)
		}
		response.WriteTo(w)
		reqLogger.Info("Script executed", "response", response, "duration", time.Since(start))
	}
}
