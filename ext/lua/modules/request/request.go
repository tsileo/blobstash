/*

Package request implements a Lua module to interact with the incoming HTTP request.

*/
package request

import (
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/tsileo/blobstash/ext/lua/luautil"
	"github.com/yuin/gopher-lua"
)

type RequestModule struct {
	uploadMaxMemory int64
	request         *http.Request
	reqId           string
	authFunc        func(*http.Request) bool
	cache           []byte // Cache the body, since it can only be streamed once
	cached          bool
}

func New(request *http.Request, reqId string, authFunc func(*http.Request) bool) *RequestModule {
	return &RequestModule{
		request:         request,
		reqId:           reqId,
		authFunc:        authFunc,
		uploadMaxMemory: 32 * 1024 * 1024, // 32MB max file uplaod FIXME(tsileo) make this configurable
	}
}

func (req *RequestModule) Loader(L *lua.LState) int {
	mod := L.SetFuncs(L.NewTable(), map[string]lua.LGFunction{
		"headers":    req.headers,
		"header":     req.header,
		"method":     req.method,
		"body":       req.body,
		"json":       req.json,
		"formdata":   req.formdata,
		"queryarg":   req.queryarg,
		"queryargs":  req.queryargs,
		"path":       req.path,
		"host":       req.host,
		"url":        req.url,
		"authorized": req.authorized,
		"upload":     req.upload,
		"hasupload":  req.hasupload,
		"remoteaddr": req.remoteaddr,
	})
	L.Push(mod)
	return 1
}

// Return a boolean indicating whether the request is authenticated using a valid API key
func (req *RequestModule) authorized(L *lua.LState) int {
	L.Push(lua.LBool(req.authFunc(req.request)))
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

// Return the HTTP request body as a string
func (req *RequestModule) body(L *lua.LState) int {
	var body string
	if req.cached {
		body = string(req.cache)
	} else {
		resp, err := ioutil.ReadAll(req.request.Body)
		if err != nil {
			panic(err)
		}
		req.cache = resp
		req.cached = true
		body = string(resp)
	}
	L.Push(lua.LString(string(body)))
	return 1
}

// Return the HTTP request body parsed as JSON
func (req *RequestModule) json(L *lua.LState) int {
	var body []byte
	if req.cached {
		body = req.cache
	} else {
		resp, err := ioutil.ReadAll(req.request.Body)
		if err != nil {
			panic(err)
		}
		body = resp
		req.cache = resp
		req.cached = true
	}
	L.Push(luautil.FromJSON(L, body))
	return 1
}

// Return the form-encoded data as a Lua table
func (req *RequestModule) formdata(L *lua.LState) int {
	luaTable := L.NewTable()
	req.request.ParseForm()
	for key, values := range req.request.Form {
		L.RawSet(luaTable, lua.LString(key), lua.LString(values[0]))
	}
	L.Push(luaTable)
	return 1
}

// Return the request referer
func (req *RequestModule) referer(L *lua.LState) int {
	res := req.request.Referer()
	L.Push(lua.LString(res))
	return 1
}

// Return the request URL user agent
func (req *RequestModule) useragent(L *lua.LState) int {
	res := req.request.UserAgent()
	L.Push(lua.LString(res))
	return 1
}

// Return the request URL
func (req *RequestModule) url(L *lua.LState) int {
	res := req.request.URL.String()
	L.Push(lua.LString(res))
	return 1
}

// Return the host component
func (req *RequestModule) host(L *lua.LState) int {
	res := req.request.URL.Host
	L.Push(lua.LString(res))
	return 1
}

// Return the path component
func (req *RequestModule) path(L *lua.LState) int {
	res := req.request.URL.Path
	L.Push(lua.LString(res))
	return 1
}

// Return the query argument for the given key
func (req *RequestModule) queryarg(L *lua.LState) int {
	res := req.request.URL.Query().Get(L.ToString(1))
	L.Push(lua.LString(res))
	return 1
}

// Return the query arguments as a Lua table
func (req *RequestModule) queryargs(L *lua.LState) int {
	luaTable := L.NewTable()
	for key, values := range req.request.Form {
		L.RawSet(luaTable, lua.LString(key), lua.LString(values[0]))
	}
	L.Push(luaTable)
	return 1
}

// Return true if request contain uploaded files
func (req *RequestModule) hasupload(L *lua.LState) int {
	L.Push(lua.LBool(req.request.Header.Get("Content-Type") == "multipart/form-data"))
	return 1
}

// Parse the request and extract a table containing form key-values,
// and a table indexed by uploaded filename returning a table with: filename, size, content key.
func (req *RequestModule) upload(L *lua.LState) int {
	if err := req.request.ParseMultipartForm(req.uploadMaxMemory); err != nil {
		// FIXME(tsileo) return a custom error so the recover can catch it
		panic(err)
	}

	valuesTable := L.NewTable()
	for key, value := range req.request.MultipartForm.Value {
		L.RawSet(valuesTable, lua.LString(key), lua.LString(value[0]))
	}

	filesTable := L.NewTable()
	for _, fileHeaders := range req.request.MultipartForm.File {
		for _, fileHeader := range fileHeaders {
			fileTable := L.NewTable()
			file, err := fileHeader.Open()
			if err != nil {
				panic(err)
			}
			L.RawSet(fileTable, lua.LString("filename"), lua.LString(fileHeader.Filename))
			buf, err := ioutil.ReadAll(file)
			if err != nil {
				panic(err)
			}
			L.RawSet(fileTable, lua.LString("size"), lua.LNumber(float64(len(buf))))
			L.RawSet(fileTable, lua.LString("content"), lua.LString(string(buf)))
			L.RawSet(filesTable, lua.LString(fileHeader.Filename), fileTable)
		}
	}
	L.Push(valuesTable)
	L.Push(filesTable)
	return 2
}

// Return the client IP
func (req *RequestModule) remoteaddr(L *lua.LState) int {
	L.Push(lua.LString(getIpAddress(req.request)))
	return 1
}

// Request.RemoteAddress contains port, which we want to remove i.e.:
// "[::1]:58292" => "[::1]"
func ipAddrFromRemoteAddr(s string) string {
	idx := strings.LastIndex(s, ":")
	if idx == -1 {
		return s
	}
	return s[:idx]
}

func getIpAddress(r *http.Request) string {
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
