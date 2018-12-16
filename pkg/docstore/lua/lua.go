package lua // import "a4.io/blobstash/pkg/docstore/lua"

import (
	"github.com/yuin/gopher-lua"

	luautil "a4.io/blobstash/pkg/apps/luautil"
	"a4.io/blobstash/pkg/docstore"
)

func setupDocStore(dc *docstore.DocStore) func(*lua.LState) int {
	return func(L *lua.LState) int {
		// register functions to the table
		mod := L.SetFuncs(L.NewTable(), map[string]lua.LGFunction{
			"col": func(L *lua.LState) int {
				name := L.ToString(1)
				ud := L.NewUserData()
				ud.Value = &col{dc, name}
				L.SetMetatable(ud, L.GetTypeMetatable("col"))
				L.Push(ud)
				return 1
			},
		})
		// returns the module
		L.Push(mod)
		return 1
	}
}

func Setup(L *lua.LState, dc *docstore.DocStore) {
	mtCol := L.NewTypeMetatable("col")
	L.SetField(mtCol, "__index", L.SetFuncs(L.NewTable(), map[string]lua.LGFunction{
		"insert": colInsert,
		"query":  colQuery,
	}))
	L.PreloadModule("docstore", setupDocStore(dc))
}

type col struct {
	dc   *docstore.DocStore
	name string
}

func checkCol(L *lua.LState) *col {
	ud := L.CheckUserData(1)
	if v, ok := ud.Value.(*col); ok {
		return v
	}
	L.ArgError(1, "col expected")
	return nil
}

func colInsert(L *lua.LState) int {
	col := checkCol(L)
	if col == nil {
		return 0
	}
	t := luautil.TableToMap(L.ToTable(2))
	id, err := col.dc.Insert(col.name, &t)
	if err != nil {
		panic(err)
	}
	L.Push(lua.LString(id.String()))
	return 1
}

func colQuery(L *lua.LState) int {
	col := checkCol(L)
	if col == nil {
		return 0
	}
	cursor := L.ToString(2)
	limit := L.ToInt(3)
	if limit == 0 {
		limit = 50
	}
	var matchFunc *lua.LFunction
	rawFunc := L.Get(4)
	if sfunc, ok := rawFunc.(lua.LString); ok {
		lhook, err := docstore.NewLuaHook(L, string(sfunc))
		if err != nil {
			panic(err)
		}
		matchFunc = lhook.LFunction()
	} else if f, ok := rawFunc.(*lua.LFunction); ok {
		matchFunc = f
	} else {
		panic("bad mathcFunc type")
	}
	docs, pointers, cursor, err := col.dc.LuaQuery(L, matchFunc, col.name, cursor, limit)
	if err != nil {
		panic(err)
	}
	L.Push(luautil.InterfaceToLValue(L, docs))
	L.Push(luautil.InterfaceToLValue(L, pointers))
	L.Push(lua.LString(cursor))
	return 3
}
