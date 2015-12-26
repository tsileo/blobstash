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
	body       *bytes.Buffer
	headers    map[string]string
	statusCode int
}

func New() *ResponseModule {
	return &ResponseModule{
		body:       bytes.NewBuffer(nil),
		statusCode: 200,
		headers:    map[string]string{},
	}
}
func (resp *ResponseModule) WriteTo(w http.ResponseWriter) error {
	for header, val := range resp.headers {
		w.Header().Set(header, val)
	}
	w.WriteHeader(resp.statusCode)
	w.Write(resp.body.Bytes())

	return nil
}

func (resp *ResponseModule) Loader(L *lua.LState) int {
	mod := L.SetFuncs(L.NewTable(), map[string]lua.LGFunction{
		"status": resp.status,
		"write":  resp.write,
		"header": resp.header,
	})
	L.Push(mod)
	return 1
}

func (resp *ResponseModule) header(L *lua.LState) int {
	resp.headers[L.ToString(1)] = L.ToString(2)
	return 0
}

func (resp *ResponseModule) status(L *lua.LState) int {
	resp.statusCode = L.ToInt(1)
	return 0
}

func (resp *ResponseModule) write(L *lua.LState) int {
	resp.body.WriteString(L.ToString(1))
	return 0
}
