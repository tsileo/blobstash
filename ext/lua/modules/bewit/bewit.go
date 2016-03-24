/*

Package bewit implement a Lua module to use Hawk bewit authentication mechanism

See https://github.com/hueniverse/hawk

*/
package bewit

import (
	"net/http"
	"net/url"
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

// Return the given URL signed with a Bewit
func (bw *BewitModule) new(L *lua.LState) int {
	// FIXME(tsileo) configurable delay for the bewit
	url, err := url.Parse(L.ToString(1))
	if err != nil {
		panic(err)
	}
	if err := httputil.NewBewit(url, time.Hour*1); err != nil {
		L.Push(lua.LString(""))
		return 1
	}
	L.Push(lua.LString(url.String()))
	return 1
}
