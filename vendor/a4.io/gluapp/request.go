package gluapp

import (
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"

	"github.com/yuin/gopher-lua"
)

// Request.RemoteAddress contains port, which we want to remove i.e.:
// "[::1]:58292" => "[::1]"
func ipAddrFromRemoteAddr(s string) string {
	idx := strings.LastIndex(s, ":")
	if idx == -1 {
		return s
	}
	return s[:idx]
}

// Return the IP Address from the `*http.Request`.
// Try the `X-Real-Ip`, `X-Forwarded-For` headers first.
func getIPAddress(r *http.Request) string {
	hdr := r.Header
	hdrRealIp := hdr.Get("X-Real-Ip")
	hdrForwardedFor := hdr.Get("X-Forwarded-For")
	if hdrRealIp == "" && hdrForwardedFor == "" {
		return ipAddrFromRemoteAddr(r.RemoteAddr)
	}
	if hdrForwardedFor != "" {
		// X-Forwarded-For is potentially a list of addresses separated with ","
		parts := strings.Split(hdrForwardedFor, ",")
		for i, p := range parts {
			parts[i] = strings.TrimSpace(p)
		}
		// TODO: should return first non-local address
		return parts[0]
	}
	return hdrRealIp
}

// TODO(tsileo): handle basic auth with fixed config + helper for request.BasicAuth wrapper

// request represents the incoming HTTP request
type request struct {
	uploadMaxMemory int64
	request         *http.Request
	body            []byte // Cache the body, since it can only be streamed once
}

func newRequest(L *lua.LState, r *http.Request) (*lua.LUserData, error) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	req := &request{
		uploadMaxMemory: 128 * 1024 * 1024, // FIXME(tsileo): move this to config
		request:         r,
		body:            body,
	}
	mt := L.NewTypeMetatable("request")
	L.SetField(mt, "__index", L.SetFuncs(L.NewTable(), map[string]lua.LGFunction{
		"body":        requestBody,
		"headers":     requestHeaders,
		"path":        requestPath,
		"remote_addr": requestRemoteAddr,
		"args":        requestArgs,
		"form":        requestForm,
		"method":      requestMethod,
		// TODO(tsileo): implements `files` (return a table like Flask) and `basic_auth`
		// "files": requestFiles,
	}))
	ud := L.NewUserData()
	ud.Value = req
	L.SetMetatable(ud, L.GetTypeMetatable("request"))
	return ud, nil
}

func checkRequest(L *lua.LState) *request {
	ud := L.CheckUserData(1)
	if v, ok := ud.Value.(*request); ok {
		return v
	}
	L.ArgError(1, "request expected")
	return nil
}

func requestBody(L *lua.LState) int {
	request := checkRequest(L)
	if request == nil {
		return 1
	}
	L.Push(buildBody(L, request.body))
	return 1
}

func requestHeaders(L *lua.LState) int {
	request := checkRequest(L)
	if request == nil {
		return 1
	}
	L.Push(buildHeaders(L, request.request.Header))
	return 1
}

func requestPath(L *lua.LState) int {
	request := checkRequest(L)
	if request == nil {
		return 1
	}
	L.Push(lua.LString(request.request.URL.Path))
	return 1
}

func requestMethod(L *lua.LState) int {
	request := checkRequest(L)
	if request == nil {
		return 1
	}
	L.Push(lua.LString(request.request.Method))
	return 1
}

func requestArgs(L *lua.LState) int {
	request := checkRequest(L)
	if request == nil {
		return 1
	}
	L.Push(buildValues(L, request.request.URL.Query()))
	return 1
}

func requestForm(L *lua.LState) int {
	request := checkRequest(L)
	if request == nil {
		return 1
	}
	values, err := url.ParseQuery(string(request.body))
	if err != nil {
		panic(err)
	}
	L.Push(buildValues(L, values))
	return 1
}

func requestRemoteAddr(L *lua.LState) int {
	request := checkRequest(L)
	if request == nil {
		return 1
	}
	L.Push(lua.LString(getIPAddress(request.request)))
	return 1
}
