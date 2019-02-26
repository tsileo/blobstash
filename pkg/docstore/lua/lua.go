package lua // import "a4.io/blobstash/pkg/docstore/lua"

import (
	"bytes"
	"html/template"

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
		"update": colUpdate,
		"insert": colInsert,
		"query":  colQuery,
		"get":    colGet,
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

func colGet(L *lua.LState) int {
	col := checkCol(L)
	if col == nil {
		return 0
	}
	docID := L.ToString(2)
	var doc, pointers map[string]interface{}
	var err error

	if _, pointers, err = col.dc.Fetch(col.name, docID, &doc, true, -1); err != nil {
		panic(err)
	}
	L.Push(luautil.InterfaceToLValue(L, doc))
	L.Push(luautil.InterfaceToLValue(L, pointers))
	return 2
}

func colUpdate(L *lua.LState) int {
	col := checkCol(L)
	if col == nil {
		return 0
	}
	docID := L.ToString(2)
	newDoc := luautil.TableToMap(L.ToTable(3))
	if err := col.dc.LuaUpdate(col.name, docID, newDoc); err != nil {
		panic(err)
	}
	return 0
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
	rtpl := L.ToString(5)
	docs, pointers, cursor, err := col.dc.LuaQuery(L, matchFunc, col.name, cursor, limit)
	if err != nil {
		panic(err)
	}
	if rtpl != "" {
		tpl, err := template.New("").Parse(rtpl)
		if err != nil {
			panic(err)
		}
		for _, doc := range docs {
			var out bytes.Buffer
			if err := tpl.Execute(&out, doc); err != nil {
				panic(err)
			}
			doc["_lua_tpl"] = out.String()
		}
	}
	L.Push(luautil.InterfaceToLValue(L, docs))
	L.Push(luautil.InterfaceToLValue(L, pointers))
	L.Push(lua.LString(cursor))
	return 3
}
