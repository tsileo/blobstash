/*

Package request implements a Lua module to interact with the incoming HTTP request.

*/
package request

import (
	"net/http"

	"github.com/yuin/gopher-lua"
)

// TODO add request id as args in New

type RequestModule struct {
	req   *http.Request
	reqId string
}

func New(req *http.Request, reqId string) *RequestModule {
	return &RequestModule{
		req:   req,
		reqId: reqId,
	}
}

func (req *RequestModule) Loader(L *lua.LState) int {
	mod := L.SetFuncs(L.NewTable(), map[string]lua.LGFunction{
		"headers": req.headers,
	})
	L.Push(mod)
	return 1
}

func (req *RequestModule) headers(L *lua.LState) int {
	luaTable := L.NewTable()
	for key := range req.req.Header {
		L.RawSet(luaTable, lua.LString(key), lua.LString(req.req.Header.Get(key)))
	}
	L.Push(luaTable)
	return 1
}
