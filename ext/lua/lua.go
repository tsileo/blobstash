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
)

const test = `
local log = require('logger')
log.info('it works')
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
		reqLogger := lua.logger.New("id", logext.RandId(8))
		reqLogger.Debug("Starting script execution")
		loggerModule := loggerModule.New(reqLogger.New("ctx", "inside script"), start)
		L := luamod.NewState()
		L.PreloadModule("logger", loggerModule.Loader)
		if err := L.DoString(test); err != nil {
			// FIXME better error, with debug mode?
			panic(err)
		}
		defer L.Close()
		// TODO(tsileo) add header reading/writing
		// w.WriteHeader(resp.Status)
		// w.Write(resp.Body)
		reqLogger.Info("Script executed", "duration", time.Since(start))
	}
}
