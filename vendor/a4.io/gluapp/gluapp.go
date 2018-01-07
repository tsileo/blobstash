package gluapp // import "a4.io/gluapp"

import (
	"fmt"
	"net/http"
	"path/filepath"

	"a4.io/blobstash/pkg/apps/luautil"
	"a4.io/gluapp/util"
	"a4.io/gluarequire2"

	"github.com/yuin/gopher-lua"
)

// TODO(tsileo): a logFunc(t time.Time, msg string, args ...interface{})?
// TODO(tsileo): an error sink ; improved error/logging/stats handling
// XXX(tsileo): unit testing support (for user, as lua script with a custom CLI for running tests)
// XXX(tsileo): cookies support?
// XXX(tsileo): a middleware method for the router?
// XXX(tsileo): a tiny package manager based on github?
// XXX(tsileo): log to a different file?

var methods = []string{
	"GET", "POST", "PUT", "PATCH", "DELETE", "TRACE", "CONNECT", "OPTIONS", "HEAD",
}

// Config represents an app configuration
type Config struct {
	// Path for looking up resources (Lua files, templates, public assets)
	Path string

	// Define the app entrypoint, default to `app.lua` (only valid for apps)
	Entrypoint string

	// HTTP client, if not set, `http.DefaultClient` will be used
	Client *http.Client

	// Hook for adding/setting additional modules/global variables
	SetupState func(L *lua.LState) error

	// Hook executed just after the script execution, just before the request is written
	AfterScriptExecHook func(L *lua.LState) error

	// Hook for custom `log` backend, defautl to `fmt.Println`
	LogHook func(logLine string) error

	// Stack trace will be displayed in debug mode
	Debug bool
}

// Setup "global" metatable (used by multiple modules)
func setupMetatable(L *lua.LState) {
	// Setup the Lua meta table for the respBody user-defined type
	mtRespBody := L.NewTypeMetatable("body")
	L.SetField(mtRespBody, "__index", L.SetFuncs(L.NewTable(), map[string]lua.LGFunction{
		"text": bodyText,
		"size": bodySize,
		"json": bodyJSON,
	}))

	// Setup the Lua meta table for the headers user-defined type
	mtHeaders := L.NewTypeMetatable("headers")
	L.SetField(mtHeaders, "__index", L.SetFuncs(L.NewTable(), map[string]lua.LGFunction{
		"add": headersAdd,
		"set": headersSet,
		"del": headersDel,
		"get": headersGet,
		"raw": headersRaw,
	}))

	// Setup the Lua meta table for the headers user-defined type
	mtValues := L.NewTypeMetatable("values")
	L.SetField(mtValues, "__index", L.SetFuncs(L.NewTable(), map[string]lua.LGFunction{
		"add": valuesAdd,
		"set": valuesSet,
		"del": valuesDel,
		"get": valuesGet,
		"raw": valuesRaw,
	}))
}

func setupState(L *lua.LState, conf *Config, w http.ResponseWriter, r *http.Request) (*Response, error) {
	// Update the path if needed
	if conf.Path != "" {
		path := L.GetField(L.GetField(L.Get(lua.EnvironIndex), "package"), "path").(lua.LString)
		path = lua.LString(conf.Path + "/?.lua;" + string(path))
		L.SetField(L.GetField(L.Get(lua.EnvironIndex), "package"), "path", lua.LString(path))
	}

	// Setup `require2`
	gluarequire2.NewRequire2Module(gluarequire2.NewRequireFromGitHub(nil)).SetGlobal(L)

	// Setup shared Lua metatables
	setupMetatable(L)

	// FIXME(tsileo): move this in a separate module, along with the "path specific" (like read_yaml" into a separate module so BlobStash can use it as a "stdlib"
	util.Setup(L, conf.Path)
	L.SetGlobal("log", L.NewFunction(func(L *lua.LState) int {
		var args []lua.LValue
		for i := 1; i <= L.GetTop(); i++ {
			item := L.Get(i)
			// We don't want table to be displayed as "table: 0xc420272240"
			if t, ok := item.(*lua.LTable); ok {
				item = lua.LString(luautil.ToJSON(t))
			}
			args = append(args, item)
		}

		// Call `string.format`
		if err := L.CallByParam(lua.P{
			Fn:      lua.LValue(L.GetField(L.GetGlobal("string"), "format").(*lua.LFunction)),
			NRet:    1,
			Protect: true,
		}, args...); err != nil {
			panic(err)
		}

		// Get the result
		logLine := string(L.Get(-1).(lua.LString))
		L.Pop(1)

		// Execute the hook
		if conf.LogHook == nil {
			fmt.Println(logLine)
		} else {
			if err := conf.LogHook(logLine); err != nil {
				panic(err)
			}
		}

		return 0
	}))

	// Setup `request`
	req, err := newRequest(L, r)
	if err != nil {
		return nil, err
	}
	// Initialize `response`
	resp, lresp := newResponse(L, w, r)

	// Set the `app` global variable
	rootTable := L.CreateTable(0, 2)
	rootTable.RawSetH(lua.LString("request"), req)
	rootTable.RawSetH(lua.LString("response"), resp)
	L.SetGlobal("app", rootTable)

	// Setup other modules
	L.PreloadModule("router", setupRouter(lresp, r.Method, r.URL.Path))
	L.PreloadModule("json", loadJSON)

	client := conf.Client
	if client == nil {
		client = http.DefaultClient
	}
	L.PreloadModule("http", setupHTTP(client, conf.Path))

	L.PreloadModule("form", setupForm()) // must be executed after setupHTTP
	L.PreloadModule("template", setupTemplate(filepath.Join(conf.Path, "templates")))
	// TODO(tsileo): a read/write file module for the data/ directory???

	// Setup additional modules provided by the user
	if conf.SetupState != nil {
		if err := conf.SetupState(L); err != nil {
			return nil, fmt.Errorf("SetupState failed: %v", err)
		}
	}

	return lresp, nil
}

// Exec run the code as a Lua script
func Exec(conf *Config, code string, w http.ResponseWriter, r *http.Request) error {
	// TODO(tsileo): clean error, take L as argument

	// Initialize a Lua state
	L := lua.NewState()
	defer L.Close()

	// Preload all the modules and setup global variables
	resp, err := setupState(L, conf, w, r)
	if err != nil {
		return err
	}

	// Execute the Lua code
	if err := L.DoString(code); err != nil {
		return err
	}

	if conf.AfterScriptExecHook != nil {
		if err := conf.AfterScriptExecHook(L); err != nil {
			return err
		}
	}

	// Write `response` content to the HTTP response
	resp.WriteTo(w)

	return nil
}
