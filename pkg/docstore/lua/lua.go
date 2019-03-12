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
			"collections": func(L *lua.LState) int {
				collections, err := dc.Collections()
				if err != nil {
					panic(err)
				}
				out := L.NewTable()
				for _, col := range collections {
					out.Append(lua.LString(col))
				}
				L.Push(out)
				return 1
			},
			"text_search": dc.LuaTextSearch,
			"setup_sort_index": func(L *lua.LState) int {
				col := L.ToString(1)
				name := L.ToString(2)
				field := L.ToString(3)
				if err := dc.LuaSetupSortIndex(col, name, field); err != nil {
					panic(err)
				}
				// FIXME(tsileo): return  true if index was created and add rebuild_sort_indexes(col)?
				return 0
			},
			"rebuild_indexes": func(L *lua.LState) int {
				col := L.ToString(1)
				if err := dc.RebuildIndexes(col); err != nil {
					panic(err)
				}
				return 0
			},
			"setup_ext": func(L *lua.LState) int {
				col := L.ToString(1)
				ext := L.ToString(2)
				data := L.ToTable(3)

				dc.SetupExt(col, ext, luautil.TableToMap(data))

				return 0
			},
			"get_ext": func(L *lua.LState) int {
				// col, ext name
				dat, err := dc.LuaGetExt(L.ToString(1), L.ToString(2))
				if err != nil {
					L.Push(lua.LNil)
					return 1
				}

				L.Push(luautil.InterfaceToLValue(L, dat))
				return 1
			},
			"setup_schema": func(L *lua.LState) int {
				name := L.ToString(1)
				fields := luautil.TableToSlice(L.ToTable(2))

				if err := dc.LuaRegisterSchema(name, fields); err != nil {
					panic(err)
				}
				return 0
			},
			"get_schema": func(L *lua.LState) int {
				name := L.ToString(1)
				schema, err := dc.LuaGetSchema(name)
				if err != nil {
					L.Push(lua.LNil)
					return 1
				}

				out := L.NewTable()

				for _, f := range schema {
					tf := L.NewTable()
					tf.RawSetString("field_name", lua.LString(f.Name))
					tf.RawSetString("field_type", lua.LString(f.Type))
					if f.Data != nil {
						tf.RawSetString("data", luautil.InterfaceToLValue(L, f.Data))
					} else {
						tf.RawSetString("data", lua.LNil)
					}
					out.Append(tf)
				}

				L.Push(out)
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
		"remove": colRemove,
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

func colRemove(L *lua.LState) int {
	col := checkCol(L)
	if col == nil {
		return 0
	}
	docID := L.ToString(2)
	if _, err := col.dc.Remove(col.name, docID); err != nil {
		panic(err)
	}
	return 0
}

func colUpdate(L *lua.LState) int {
	col := checkCol(L)
	if col == nil {
		return 0
	}
	docID := L.ToString(2)
	newDoc := luautil.TableToMap(L.ToTable(3))

	if _, err := col.dc.Update(col.name, docID, newDoc, ""); err != nil {
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
	sortIndex := L.ToString(5)
	docs, pointers, cursor, err := col.dc.LuaQuery(L, matchFunc, col.name, cursor, sortIndex, limit)
	if err != nil {
		panic(err)
	}
	L.Push(luautil.InterfaceToLValue(L, docs))
	L.Push(luautil.InterfaceToLValue(L, pointers))
	L.Push(lua.LString(cursor))
	return 3
}
