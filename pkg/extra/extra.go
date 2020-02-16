package extra // import "a4.io/blobstash/pkg/extra"

import (
	"path/filepath"
	"strings"
	"time"

	"github.com/yuin/gopher-lua"
)

type Extra struct {
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
			"format_datetime": func(L *lua.LState) int {
				dt := L.ToString(1)
				layout := L.ToString(2)
				t, err := time.Parse(layout, dt)
				if err != nil {
					panic(err)
				}

				L.Push(lua.LString(t.Format(L.ToString(3))))

				return 1
			},
			"split": func(L *lua.LState) int {
				tbl := L.NewTable()
				for _, part := range strings.Split(L.ToString(1), L.ToString(2)) {
					tbl.Append(lua.LString(part))
				}
				L.Push(tbl)
				return 1
			},
		})
		// returns the module
		L.Push(mod)
		return 1
	}
}

func Setup(L *lua.LState) *Extra {
	e := &Extra{}
	// luautil.InterfaceToLValue(L, nil)
	L.PreloadModule("extra", setupExtra(e))
	return e
}
