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
	req *http.Request
}

func New(req *http.Request) *RequestModule {
	return &RequestModule{
		req: req,
	}
}

func (req *RequestModule) Loader(L *lua.LState) int {
	mod := L.SetFuncs(L.NewTable(), map[string]lua.LGFunction{})
	L.Push(mod)
	return 1
}
