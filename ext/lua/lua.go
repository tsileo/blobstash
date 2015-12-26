package lua

import (
	"net/http"
	"time"

	_ "github.com/cjoudrey/gluahttp"
	"github.com/gorilla/mux"
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

log.info('it works')
resp.write('Nothing to see here!')
resp.header('My-Header', 'value')
resp.status(404)
log.info(req.headers())
`

// TODO(tsileo) Load script from filesystem/laoded via HTTP POST
// TODO(tsileo) Find a way to give unique url to script: UUID?

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

func (lua *LuaExt) ScriptHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		// httpClient := &http.Client{}
		start := time.Now()
		reqId := logext.RandId(8)
		reqLogger := lua.logger.New("id", reqId)
		reqLogger.Debug("Starting script execution")
		logger := loggerModule.New(reqLogger.New("ctx", "inside script"), start)
		response := responseModule.New()
		request := requestModule.New(r, reqId)
		L := luamod.NewState()
		L.PreloadModule("request", request.Loader)
		L.PreloadModule("response", response.Loader)
		L.PreloadModule("logger", logger.Loader)
		if err := L.DoString(test); err != nil {
			// FIXME better error, with debug mode?
			panic(err)
		}
		defer L.Close()
		response.WriteTo(w)
		// TODO(tsileo) add header reading/writing
		reqLogger.Info("Script executed", "response", response, "duration", time.Since(start))
	}
}
