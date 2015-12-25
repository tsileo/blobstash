package lua

import (
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/stevedonovan/luar"
	log "gopkg.in/inconshreveable/log15.v2"
	logext "gopkg.in/inconshreveable/log15.v2/ext"
)

const test = `
resp.SetStatus(404)
log("it works")
resp.Write("Not Found")
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
		start := time.Now()
		reqLogger := lua.logger.New("id", logext.RandId(8))
		reqLogger.Debug("Starting script execution")
		log := func(msg string) {
			reqLogger.Debug(msg, "t", time.Since(start), "context", "Lua script")
		}
		L := luar.Init()
		defer L.Close()
		resp := &Resp{}
		luar.Register(L, "", luar.Map{
			"resp": resp,
			"log":  log,
		})

		L.DoString(test)
		// TODO(tsileo) add header reading/writing
		w.WriteHeader(resp.Status)
		w.Write(resp.Body)
		reqLogger.Info("Script executed", "resp", resp, "duration", time.Since(start))

	}
}
