/*

Package response implements a Lua module to interact with an HTTP response.

*/
package response

import (
	"bytes"
	"html/template"
	"net/http"

	"github.com/tsileo/blobstash/ext/lua/luautil"
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
		"status":       resp.status,
		"write":        resp.write,
		"jsonify":      resp.jsonify,
		"header":       resp.header,
		"error":        resp.error,
		"authenticate": resp.authenticate,
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

func (resp *ResponseModule) Status() int {
	return resp.statusCode
}

// Output JSON (with the right Content-Type), the data must be a table (or use `json` module with write).
func (resp *ResponseModule) jsonify(L *lua.LState) int {
	js := luautil.ToJSON(L.ToTable(1))
	resp.body.Write(js)
	resp.headers["Content-Type"] = "application/json"
	return 0
}

// Return an error with the given status code and an optional error message
func (resp *ResponseModule) error(L *lua.LState) int {
	status := int(L.ToNumber(1))
	resp.statusCode = status

	var message string
	if L.GetTop() == 2 {
		message = L.ToString(2)
	} else {
		message = http.StatusText(status)
	}
	resp.body.Reset()
	resp.body.WriteString(message)
	return 0
}

// Set the header for asking Basic Auth credentials (with the given realm)
func (resp *ResponseModule) authenticate(L *lua.LState) int {
	resp.headers["WWW-Authenticate"] = "Basic realm=\"" + L.ToString(1) + "\""
	return 0
}

// Render execute a Go HTML template, data must be a table with string keys
func (resp *ResponseModule) render(L *lua.LState) int {
	tplString := L.ToString(1)
	data := luautil.TableToMap(L.ToTable(2))
	tpl, err := template.New("tpl").Parse(tplString)
	if err != nil {
		L.Push(lua.LString(err.Error()))
		return 1
	}
	// TODO(tsileo) add some templatFuncs/template filter
	out := &bytes.Buffer{}
	if err := tpl.Execute(out, data); err != nil {
		L.Push(lua.LString(err.Error()))
		return 1
	}
	L.Push(lua.LString(out.String()))
	return 1
}
