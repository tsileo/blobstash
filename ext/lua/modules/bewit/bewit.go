/*

Package bewit implement a Lua module to use Hawk bewit authentication mechanism

See https://github.com/hueniverse/hawk

*/
package bewit

import (
	"net/http"
	"time"

	"github.com/tsileo/blobstash/httputil"
	"github.com/yuin/gopher-lua"
	log "gopkg.in/inconshreveable/log15.v2"
)

type BewitModule struct {
	logger log.Logger
	req    *http.Request
}

func New(logger log.Logger, req *http.Request) *BewitModule {
	return &BewitModule{
		logger: logger,
		req:    req,
	}
}

func (bw *BewitModule) Loader(L *lua.LState) int {
	mod := L.SetFuncs(L.NewTable(), map[string]lua.LGFunction{
		"new":   bw.new,
		"check": bw.check,
	})
	L.Push(mod)
	return 1
}

// Try to authenticate the request
func (bw *BewitModule) check(L *lua.LState) int {
	err := httputil.CheckBewit(bw.req)
	out := ""
	if err != nil {
		out = err.Error()
	}
	L.Push(lua.LString(out))
	return 1
}

// Return a new bewit token for the given URL valid for the given delay
func (bw *BewitModule) new(L *lua.LState) int {
	// FIXME(tsileo) configurable delay for the bewit
	token, err := httputil.NewBewit(L.ToString(1), time.Hour*1)
	if err != nil {
		L.Push(lua.LString(""))
		return 1
	}
	L.Push(lua.LString(token))
	bw.logger.Info("Bewit generated", "token", token)
	return 1
}
