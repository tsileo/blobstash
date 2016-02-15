/*

Package docstore implements a Lua module to interact with the docstore extension.

*/
package docstore

import (
	"github.com/yuin/gopher-lua"

	docstoreExt "github.com/tsileo/blobstash/ext/docstore"
	"github.com/tsileo/blobstash/ext/lua/luautil"
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
	col := &Col{ds.docstore, L.CheckString(1)}
	ud := L.NewUserData()
	ud.Value = col
	L.SetMetatable(ud, L.GetTypeMetatable(luaColTypeName))
	L.Push(ud)
	return 1
}

type Col struct {
	docstore *docstoreExt.DocStoreExt
	Name     string
}

const luaColTypeName = "col"

func registerColType(L *lua.LState) {
	mt := L.NewTypeMetatable(luaColTypeName)
	L.SetGlobal("col", mt)
	// static attributes
	// L.SetField(mt, "new", L.NewFunction(newCol))
	// methods
	L.SetField(mt, "__index", L.SetFuncs(L.NewTable(), colMethods))
}

// Constructor
// FIXME(tsileo): ensure the `new` field isn't required
// func newCol(L *lua.LState) int {
// 	col := &Col{L.CheckString(1)}
// 	ud := L.NewUserData()
// 	ud.Value = col
// 	L.SetMetatable(ud, L.GetTypeMetatable(luaColTypeName))
// 	L.Push(ud)
// 	return 1
// }

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
	"name":   colGetName,
	"insert": colInsert,
	"get":    colGet,
	// "query":  colQuery,
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

func colInsert(L *lua.LState) int {
	c := checkCol(L)
	doc := L.CheckTable(2)
	opts := L.ToTable(3)
	var ns string
	var index bool
	if opts != nil {
		if tns := L.RawGet(opts, lua.LString("namespace")); tns != lua.LNil {
			if sns, ok := tns.(lua.LString); ok {
				ns = string(sns)
			}
		}
		if tindex := L.RawGet(opts, lua.LString("index")); tindex != lua.LNil {
			if bindex, ok := tindex.(lua.LBool); ok {
				if lua.LVAsBool(bindex) {
					index = true
				}
			}
		}
	}
	table := luautil.TableToMap(doc)
	_id, err := c.docstore.Insert(c.Name, &table, ns, index)
	if err != nil {
		panic(err)
	}
	L.Push(lua.LString(_id.String()))
	return 1
}

func colGet(L *lua.LState) int {
	c := checkCol(L)
	res := map[string]interface{}{}
	_id := L.CheckString(2)
	if _, err := c.docstore.Fetch(c.Name, _id, &res); err != nil {
		panic(err)
	}
	L.Push(luautil.InterfaceToLValue(L, res))
	return 1
}
