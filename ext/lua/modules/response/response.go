/*

Package response implements a Lua module to interact with an HTTP response.

*/
package response

import (
	"net/http"

	"github.com/yuin/gopher-lua"
)

type ResponseModule struct {
	resp *http.Response
}

func New(resp *http.Response) *ResponseModule {
	return &ResponseModule{
		resp: resp,
	}
}

func (resp *ResponseModule) Loader(L *lua.LState) int {
	mod := L.SetFuncs(L.NewTable(), map[string]lua.LGFunction{})
	L.Push(mod)
	return 1
}
