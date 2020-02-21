/*

Package luautil implements utility for gopher-lua.

*/
package luautil // import "a4.io/blobstash/pkg/apps/luautil"
// TODO(tsileo): move it to pkg/luautil

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/yuin/gopher-lua"
)

// FIXME(tsileo): support a custom metatable so `jsonutil.new_list/map()` and `jsonutil.is_list/map()` can work
// and jsonuti.new_list() can be dumped as an empty list via json.encode. Or use __index for __jsonutil_type "map"|"list" and a global "map"/"list" metatable from json
// map VS object, list VS array

// AddToPath append the given path the the Lua package.path
func AddToPath(L *lua.LState, npath string) {
	path := L.GetField(L.GetField(L.Get(lua.EnvironIndex), "package"), "path").(lua.LString)
	path = lua.LString(npath + "/?.lua;" + string(path))
	L.SetField(L.GetField(L.Get(lua.EnvironIndex), "package"), "path", lua.LString(path))
}

// TableToMap converts a `*lua.LTable` to a `map[string]interface{}`
func TableToMap(L *lua.LState, table *lua.LTable) map[string]interface{} {
	res, _ := tomap(L, table, map[*lua.LTable]bool{})
	return res
}

// TableToSlice converts a `*lua.LTable` to a `[]interface{}`
func TableToSlice(L *lua.LState, table *lua.LTable) []interface{} {
	_, res := tomap(L, table, map[*lua.LTable]bool{})
	return res
}

func tomap(L *lua.LState, table *lua.LTable, visited map[*lua.LTable]bool) (map[string]interface{}, []interface{}) {
	res := map[string]interface{}{}
	var arrres []interface{}
	nkey := false
	table.ForEach(func(key lua.LValue, value lua.LValue) {
		_, numberKey := key.(lua.LNumber)
		if numberKey {
			nkey = true
		}
		switch converted := value.(type) {
		case lua.LBool:
			val := false
			if converted == lua.LTrue {
				val = true
			}
			if nkey {
				arrres = append(arrres, val)
			} else {
				res[key.String()] = val
			}
		case lua.LString:
			if nkey {
				arrres = append(arrres, string(converted))
			} else {
				res[key.String()] = string(converted)
			}
		case lua.LNumber:
			if nkey {
				arrres = append(arrres, float64(converted))
			} else {
				res[key.String()] = float64(converted)
			}
		case lua.LChannel:
			panic("no channel")
		case *lua.LFunction:
			panic("no function")
		case *lua.LNilType:
			res[key.String()] = converted
		case *lua.LState:
			panic("no LState")
		case *lua.LTable:
			// if visited[converted] {
			//	panic("nested table")
			// }
			visited[converted] = true
			stres, sares := tomap(L, converted, visited)
			if nkey {
				if sares != nil {
					arrres = append(arrres, sares)
				} else {
					arrres = append(arrres, stres)
				}
			} else {
				if sares != nil {
					res[key.String()] = sares
				} else {
					res[key.String()] = stres
				}
			}
		}
	})
	if arrres == nil && L.GetMetatable(table) == L.GetGlobal("__gluapp_json_array") {
		return nil, []interface{}{}
	}
	return res, arrres
}

// Convert a Lua table to JSON
// Adapted from https://github.com/layeh/gopher-json/blob/master/util.go (Public domain)
func ToJSON(L *lua.LState, value lua.LValue) []byte {
	var data []byte
	var err error
	switch converted := value.(type) {
	case lua.LBool:
		data, err = json.Marshal(converted)
	case lua.LChannel:
		err = errors.New("ToJSON: cannot marshal channel")
	case lua.LNumber:
		data, err = json.Marshal(converted)
	case *lua.LFunction:
		err = errors.New("ToJSON: cannot marshal function")
	case *lua.LNilType:
		data, err = json.Marshal(converted)
	case *lua.LState:
		err = errors.New("ToJSON: cannot marshal LState")
	case lua.LString:
		data, err = json.Marshal(converted)
	case *lua.LTable:
		res, ares := tomap(L, converted, map[*lua.LTable]bool{})
		if ares != nil {
			data, err = json.Marshal(ares)
		} else {
			data, err = json.Marshal(res)
		}
	case *lua.LUserData:
		err = errors.New("ToJSON: cannot marshal user data")
		// TODO: call metatable __tostring?
	}
	if err != nil {
		panic(err)
	}
	return data
}

// Convert the JSON to a Lua object ready to be pushed
// Adapted from https://github.com/layeh/gopher-json/blob/master/util.go (Public domain)
func FromJSON(L *lua.LState, js []byte) lua.LValue {
	var res interface{}
	if err := json.Unmarshal(js, &res); err != nil {
		panic(err)
	}
	return fromJSON(L, res)
}

// InterfaceToLValue converts the given value to its `lua.LValue` counterpart
func InterfaceToLValue(L *lua.LState, value interface{}) lua.LValue {
	return fromJSON(L, value)
}

func fromJSON(L *lua.LState, value interface{}) lua.LValue {
	// XXX it handles so many types because the docstore uses msgpack (and it handles all the possible types)
	switch converted := value.(type) {
	case bool:
		return lua.LBool(converted)
	case int8:
		return lua.LNumber(converted)
	case int16:
		return lua.LNumber(converted)
	case int32:
		return lua.LNumber(converted)
	case uint8:
		return lua.LNumber(converted)
	case uint16:
		return lua.LNumber(converted)
	case uint32:
		return lua.LNumber(converted)
	case uint64:
		return lua.LNumber(converted)
	case int:
		return lua.LNumber(converted)
	case int64:
		return lua.LNumber(converted)
	case float64:
		return lua.LNumber(converted)
	case string:
		return lua.LString(converted)
	case []interface{}:
		arr := L.CreateTable(len(converted), 0)
		for _, item := range converted {
			arr.Append(fromJSON(L, item))
		}
		mt := L.GetGlobal("__gluapp_json_array")
		if mt != lua.LNil {
			L.SetMetatable(arr, mt)
		}
		return arr
	case []map[string]interface{}:
		arr := L.CreateTable(len(converted), 0)
		for _, item := range converted {
			arr.Append(fromJSON(L, item))
		}
		return arr
	case map[string]interface{}:
		mt := L.GetGlobal("__gluapp_json_object")
		tbl := L.CreateTable(0, len(converted))
		for key, item := range converted {
			tbl.RawSetH(lua.LString(key), fromJSON(L, item))
		}
		if mt != lua.LNil {
			L.SetMetatable(tbl, mt)
		}
		return tbl
	case nil:
		return lua.LNil
	default:
		// FIXME(tsileo): create a parser for `lua:"key_name"` and build a table by iterating the field a converting value!
		// or a ToLua(L) lua.LValue interface?
		if s, ok := converted.(fmt.Stringer); ok {
			return lua.LString(s.String())
		}
		js, err := json.Marshal(converted)
		if err == nil {
			return FromJSON(L, js)
		}
		panic(fmt.Errorf("unsupported type %+v", converted))
	}
}
