/*

Package request implements a Lua module to interact with the incoming HTTP request.

*/
package request

import (
	"io/ioutil"
	"net/http"

	"github.com/yuin/gopher-lua"
)

// TODO add request id as args in New

type RequestModule struct {
	request *http.Request
	reqId   string
}

func New(request *http.Request, reqId string) *RequestModule {
	return &RequestModule{
		request: request,
		reqId:   reqId,
	}
}

func (req *RequestModule) Loader(L *lua.LState) int {
	mod := L.SetFuncs(L.NewTable(), map[string]lua.LGFunction{
		"headers":  req.headers,
		"header":   req.header,
		"method":   req.method,
		"body":     req.body,
		"formdata": req.formdata,
	})
	L.Push(mod)
	return 1
}

// Return the HTTP method (GET, POST...)
func (req *RequestModule) method(L *lua.LState) int {
	L.Push(lua.LString(req.request.Method))
	return 1
}

// Return the HTTP header for the given key
func (req *RequestModule) header(L *lua.LState) int {
	L.Push(lua.LString(req.request.Header.Get(L.ToString(1))))
	return 1
}

// Return all the HTTP headers as a table
func (req *RequestModule) headers(L *lua.LState) int {
	luaTable := L.NewTable()
	for key := range req.request.Header {
		L.RawSet(luaTable, lua.LString(key), lua.LString(req.request.Header.Get(key)))
	}
	L.Push(luaTable)
	return 1
}

// Return the HTTP request body as a string. Can only be called once.
func (req *RequestModule) body(L *lua.LState) int {
	body, err := ioutil.ReadAll(req.request.Body)
	if err != nil {
		L.Push(lua.LString(""))
		L.Push(lua.LString(err.Error()))
	}
	L.Push(lua.LString(string(body)))
	L.Push(lua.LString(""))
	return 2
}

// Return the form-encoded data as a Lua tabl
func (req *RequestModule) formdata(L *lua.LState) int {
	luaTable := L.NewTable()
	req.request.ParseForm()
	for key, values := range req.request.Form {
		L.RawSet(luaTable, lua.LString(key), lua.LString(values[0]))
	}
	L.Push(luaTable)
	return 1
}
