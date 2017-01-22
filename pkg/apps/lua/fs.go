/*

Package response implements a Lua module to interact with an HTTP response.

*/
package lua // import "a4.io/blobstash/pkg/apps/lua"

import (
	"io/ioutil"
	"path/filepath"

	_ "a4.io/blobstash/pkg/apps/luatuil"
	"github.com/yuin/gopher-lua"
)

type FSModule struct {
	root string
}

func NewFSModule(path string) *FSModule {
	return &FSModule{
		root: path,
	}
}

func (fs *FSModule) Loader(L *lua.LState) int {
	// TODO(tsileo): implement write
	mod := L.SetFuncs(L.NewTable(), map[string]lua.LGFunction{
		"read": fs.read,
	})
	L.Push(mod)
	return 1
}

// Return an error with the given status code and an optional error message
func (fs *FSModule) read(L *lua.LState) int {
	data, err := ioutil.ReadFile(filepath.Join(fs.root, L.ToString(1)))
	if err != nil {
		panic(err)
	}
	L.Push(lua.LString(data))
	return 1
}
