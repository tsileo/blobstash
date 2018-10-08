package extra // import "a4.io/blobstash/pkg/extra"

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"path/filepath"

	"github.com/yuin/gopher-lua"
)

type Extra struct {
	resourceCache map[string]lua.LString
}

func setupExtra(e *Extra) func(*lua.LState) int {
	return func(L *lua.LState) int {
		// register functions to the table
		mod := L.SetFuncs(L.NewTable(), map[string]lua.LGFunction{
			"noop": func(L *lua.LState) int {
				return 0
			},
			"glob": func(L *lua.LState) int {
				// match(<glob pattern>, <name>)
				matched, err := filepath.Match(L.ToString(1), L.ToString(2))
				if err != nil {
					panic(err)
				}
				if matched {
					L.Push(lua.LTrue)
				} else {
					L.Push(lua.LFalse)
				}
				return 1
			},
			"embed_http_resource": func(L *lua.LState) int {
				url := L.ToString(1)

				resp, err := http.Get(url)
				if err != nil {
					panic(fmt.Errorf("failed to fetch URL: %s: %s", url, err))
				}
				defer resp.Body.Close()

				body, err := ioutil.ReadAll(resp.Body)
				if err != nil {
					panic(fmt.Errorf("failed to read response: %s", err))
				}

				data := lua.LString(body)

				e.resourceCache[url] = data

				L.Push(data)
				return 1
			},
		})
		// returns the module
		L.Push(mod)
		return 1
	}
}

func Setup(L *lua.LState) *Extra {
	e := &Extra{
		resourceCache: map[string]lua.LString{},
	}
	// luautil.InterfaceToLValue(L, nil)
	L.PreloadModule("extra", setupExtra(e))
	return e
}
