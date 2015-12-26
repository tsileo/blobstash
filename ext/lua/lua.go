package lua

import (
	"net/http"
	"time"

	_ "github.com/cjoudrey/gluahttp"
	"github.com/gorilla/mux"
	"github.com/yuin/gopher-lua"
	log "gopkg.in/inconshreveable/log15.v2"
	logext "gopkg.in/inconshreveable/log15.v2/ext"
)

const test = `
local log = require('logger')
log.info('it works')
`

type Resp struct {
	Body   []byte
	Status int
}

func NewResp() *Resp {
	return &Resp{
		Status: 200,
	}
}

func (r *Resp) SetStatus(status int) {
	r.Status = status
}

func (r *Resp) Write(data string) {
	r.Body = append(r.Body, []byte(data)...)
}

// TODO(tsileo) Load script from filesystem/laoded via HTTP POST
// TODO(tsileo) Find a way to give unique url to script: UUID?

type LuaExt struct {
	luaPool *lStatePool

	logger log.Logger
}

func New(logger log.Logger) *LuaExt {
	luaPool := &lStatePool{
		saved: make([]*lua.LState, 0, 4),
	}
	return &LuaExt{
		luaPool: luaPool,
		logger:  logger,
	}
}

func (lua *LuaExt) RegisterRoute(r *mux.Router) {
	r.HandleFunc("/", lua.ScriptHandler())
}

type LoggerModule struct {
	logger log.Logger
	start  time.Time
}

func (logger *LoggerModule) Loader(L *lua.LState) int {
	// register functions to the table
	exports := map[string]lua.LGFunction{
		"info": logger.info,
	}
	mod := L.SetFuncs(L.NewTable(), exports)
	// register other stuff
	// L.SetField(mod, "name", lua.LString("value"))

	// returns the module
	L.Push(mod)
	return 1
}

func (logger *LoggerModule) info(L *lua.LState) int {
	logger.logger.Info(L.ToString(1), "t", time.Since(logger.start))
	return 0
}

func (lua *LuaExt) ScriptHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		// httpClient := &http.Client{}
		start := time.Now()
		reqLogger := lua.logger.New("id", logext.RandId(8))
		reqLogger.Debug("Starting script execution")
		loggerModule := &LoggerModule{
			start:  start,
			logger: reqLogger.New("ctx", "inside script"),
		}
		L := lua.luaPool.Get()
		L.PreloadModule("logger", loggerModule.Loader)
		if err := L.DoString(test); err != nil {
			// FIXME better error, with debug mode?
			panic(err)
		}
		defer lua.luaPool.Put(L)
		resp := &Resp{Status: 200}
		// TODO(tsileo) add header reading/writing
		w.WriteHeader(resp.Status)
		w.Write(resp.Body)
		reqLogger.Info("Script executed", "resp", resp, "duration", time.Since(start))

	}
}
