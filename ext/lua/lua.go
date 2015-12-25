package lua

import (
	"fmt"
	"github.com/gorilla/mux"
	"github.com/stevedonovan/luar"
	log "gopkg.in/inconshreveable/log15.v2"
	logext "gopkg.in/inconshreveable/log15.v2/ext"
	"net/http"
)

const test = `
for i = 1,10 do
    Print(MSG,i)
end
resp.SetStatus(404)
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
		reqLogger := lua.logger.New("id", logext.RandId(8))
		L := luar.Init()
		defer L.Close()
		resp := &Resp{}
		luar.Register(L, "", luar.Map{
			"resp":  resp,
			"Print": fmt.Println,
		})

		L.DoString(test)
		// TODO(tsileo) add header reading/writing
		w.WriteHeader(resp.Status)
		w.Write(resp.Body)
		reqLogger.Debug("resp", "resp", resp)

	}
}
