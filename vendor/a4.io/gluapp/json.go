package gluapp

import (
	"a4.io/blobstash/pkg/apps/luautil"

	"github.com/yuin/gopher-lua"
)

func addMagicField(L *lua.LState, mt *lua.LTable, k, v string) {
	L.SetField(mt, "__index", L.NewFunction(func(ls *lua.LState) int {
		if ls.ToString(2) == k {
			ls.Push(lua.LString(v))
			return 1
		}
		ls.Push(lua.LNil)
		return 1
	}))
}

func isMetatable(mt *lua.LTable) func(*lua.LState) int {
	return func(L *lua.LState) int {
		if L.GetMetatable(L.ToTable(1)) == mt {
			L.Push(lua.LTrue)
		} else {
			L.Push(lua.LFalse)
		}
		return 1
	}
}

func loadJSON(L *lua.LState) int {
	mt_object := L.NewTypeMetatable("__gluap_json_object")
	addMagicField(L, mt_object, "__gluap_json_type", "object")
	L.SetGlobal("__gluap_json_object", mt_object)
	mt_array := L.NewTypeMetatable("__gluapp_json_array")
	addMagicField(L, mt_array, "__gluap_json_type", "array")
	L.SetGlobal("__gluapp_json_array", mt_array)

	// register functions to the table
	mod := L.SetFuncs(L.NewTable(), map[string]lua.LGFunction{
		"decode":    jsonDecode,
		"encode":    jsonEncode,
		"is_object": isMetatable(mt_object),
		"is_array":  isMetatable(mt_array),
		"new_object": func(L *lua.LState) int {
			tbl := L.NewTable()
			L.SetMetatable(tbl, mt_object)
			L.Push(tbl)
			return 1
		},
		"new_array": func(L *lua.LState) int {
			tbl := L.NewTable()
			L.SetMetatable(tbl, mt_array)
			L.Push(tbl)
			return 1
		},
	})
	// returns the module
	L.Push(mod)
	return 1
}

func jsonEncode(L *lua.LState) int {
	data := L.CheckAny(1)
	if data == nil {
		L.Push(lua.LNil)
		return 1
	}
	L.Push(lua.LString(string(luautil.ToJSON(L, data))))
	return 1
}

// TODO(tsileo): a note about empty list vs empty object
func jsonDecode(L *lua.LState) int {
	data := L.ToString(1)
	L.Push(luautil.FromJSON(L, []byte(data)))
	return 1
}
