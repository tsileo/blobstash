package lua

import (
	"bytes"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"time"

	"github.com/cjoudrey/gluahttp"
	"github.com/dchest/blake2b"
	"github.com/gorilla/mux"
	luajson "github.com/layeh/gopher-json"
	"github.com/russross/blackfriday"
	"github.com/tsileo/blobstash/client/interface"
	luamod "github.com/yuin/gopher-lua"
	log "gopkg.in/inconshreveable/log15.v2"
	logext "gopkg.in/inconshreveable/log15.v2/ext"

	bewitModule "github.com/tsileo/blobstash/ext/lua/modules/bewit"
	blobstoreModule "github.com/tsileo/blobstash/ext/lua/modules/blobstore"
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

// log.info(string.format('body=%s', body))
// TODO(tsileo) Load script from filesystem/laoded via HTTP POST
// TODO(tsileo) Find a way to give unique url to script: UUID?

type LuaExt struct {
	logger log.Logger

	blobStore client.BlobStorer
}

func New(logger log.Logger, blobStore client.BlobStorer) *LuaExt {
	return &LuaExt{
		logger:    logger,
		blobStore: blobStore,
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

	// Render execute a Go template, data must be encoded as JSON
	L.SetGlobal("render", L.NewFunction(func(L *luamod.LState) int {
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
		blobstore := blobstoreModule.New(lua.blobStore)
		bewit := bewitModule.New(reqLogger.New("ctx", "bewit module"), r)

		// Initialize Lua state
		L := luamod.NewState()
		defer L.Close()
		setCustomGlobals(L)
		L.PreloadModule("request", request.Loader)
		L.PreloadModule("response", response.Loader)
		L.PreloadModule("logger", logger.Loader)
		L.PreloadModule("blobstore", blobstore.Loader)
		L.PreloadModule("bewit", bewit.Loader)

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
