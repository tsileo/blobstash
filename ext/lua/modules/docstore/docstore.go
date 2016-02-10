/*

Package docstore implements a Lua module to interact with the docstore extension.

*/
package docstore

import (
	docstoreExt "github.com/tsileo/blobstash/ext/docstore"
	"github.com/yuin/gopher-lua"
)

type DocstoreModule struct {
	docstore *docstoreExt.DocStoreExt
}

func New(docstore *docstoreExt.DocStoreExt) *DocstoreModule {
	return &DocstoreModule{
		docstore: docstore,
	}

}

func (ds *DocstoreModule) Loader(L *lua.LState) int {
	registerColType(L)
	mod := L.SetFuncs(L.NewTable(), map[string]lua.LGFunction{
		"col": ds.col,
	})
	L.Push(mod)
	return 1
}

func (ds *DocstoreModule) col(L *lua.LState) int {
	col := &Col{L.CheckString(1)}
	ud := L.NewUserData()
	ud.Value = col
	L.SetMetatable(ud, L.GetTypeMetatable(luaColTypeName))
	L.Push(ud)
	return 1
}

type Col struct {
	Name string
}

const luaColTypeName = "col"

func registerColType(L *lua.LState) {
	mt := L.NewTypeMetatable(luaColTypeName)
	L.SetGlobal("col", mt)
	// static attributes
	L.SetField(mt, "new", L.NewFunction(newCol))
	// methods
	L.SetField(mt, "__index", L.SetFuncs(L.NewTable(), colMethods))
}

// Constructor
func newCol(L *lua.LState) int {
	col := &Col{L.CheckString(1)}
	ud := L.NewUserData()
	ud.Value = col
	L.SetMetatable(ud, L.GetTypeMetatable(luaColTypeName))
	L.Push(ud)
	return 1
}

// Checks whether the first lua argument is a *LUserData with *Person and returns this *Person.
func checkCol(L *lua.LState) *Col {
	ud := L.CheckUserData(1)
	if v, ok := ud.Value.(*Col); ok {
		return v
	}
	L.ArgError(1, "col expected")
	return nil
}

var colMethods = map[string]lua.LGFunction{
	"name": colGetName,
}

// Getter and setter for the Person#Name
func colGetName(L *lua.LState) int {
	c := checkCol(L)
	// if L.GetTop() == 2 {
	// 	p.Name = L.CheckString(2)
	// 	return 0
	// }
	L.Push(lua.LString(c.Name))
	return 1
}
