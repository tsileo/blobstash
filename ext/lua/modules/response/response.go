/*

Package response implements a Lua module to interact with an HTTP response.

*/
package response

import (
	"bytes"
	"net/http"

	"github.com/yuin/gopher-lua"
)

type ResponseModule struct {
	Body   *bytes.Buffer
	Status int
}

func New() *ResponseModule {
	return &ResponseModule{
		Body:   bytes.NewBuffer(nil),
		Status: 200,
	}
}
func (resp *ResponseModule) WriteTo(w http.ResponseWriter) error {
	w.WriteHeader(resp.Status)
	w.Write(resp.Body.Bytes())
	return nil
}

func (resp *ResponseModule) Loader(L *lua.LState) int {
	mod := L.SetFuncs(L.NewTable(), map[string]lua.LGFunction{
		"status": resp.status,
		"write":  resp.write,
	})
	L.Push(mod)
	return 1
}

func (resp *ResponseModule) status(L *lua.LState) int {
	resp.Status = L.ToInt(1)
	return 0
}

func (resp *ResponseModule) write(L *lua.LState) int {
	resp.Body.WriteString(L.ToString(1))
	return 0
}
