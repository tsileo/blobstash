package kvstore

import (
	"github.com/tsileo/blobstash/client/interface"
	"github.com/tsileo/blobstash/client/response"
	"github.com/tsileo/blobstash/ext/lua/luautil"
	"github.com/yuin/gopher-lua"
)

func kvToTable(L *lua.LState, kv *response.KeyValue) *lua.LTable {
	kvTable := L.NewTable()
	L.RawSet(kvTable, lua.LString("key"), lua.LString(kv.Key))
	L.RawSet(kvTable, lua.LString("value"), lua.LString(kv.Value))
	L.RawSet(kvTable, lua.LString("version"), lua.LNumber(float64(kv.Version)))
	return kvTable
}

type KvStoreModule struct {
	kvStore client.KvStorer
}

func New(kvStore client.KvStorer) *KvStoreModule {
	return &KvStoreModule{kvStore}
}

func (kvs *KvStoreModule) Loader(L *lua.LState) int {
	mod := L.SetFuncs(L.NewTable(), map[string]lua.LGFunction{
		"get":      kvs.get,
		"put":      kvs.put,
		"keys":     kvs.keys,
		"versions": kvs.versions,
		"getjson":  kvs.getjson,
		"putjson":  kvs.putjson,
		"keysjson": kvs.keysjson,
	})
	L.Push(mod)
	return 1
}

func (kvs *KvStoreModule) versions(L *lua.LState) int {
	luaTable := L.NewTable()
	kvResp, err := kvs.kvStore.Versions(L.ToString(1), L.ToInt(2), L.ToInt(3), L.ToInt(4))
	if err != nil {
		panic(err)
	}
	for index, kv := range kvResp.Versions {
		L.RawSetInt(luaTable, index+1, kvToTable(L, kv))
	}
	L.Push(luaTable)
	return 1
}

func (kvs *KvStoreModule) keysjson(L *lua.LState) int {
	luaTable := L.NewTable()
	kvResp, err := kvs.kvStore.Keys(L.ToString(1), L.ToString(2), L.ToInt(3))
	if err != nil {
		panic(err)
	}
	for index, kv := range kvResp {
		kvTable := kvToTable(L, kv)
		L.RawSet(kvTable, lua.LString("value"), luautil.FromJSON(L, []byte(kv.Value)))
		L.RawSetInt(luaTable, index+1, kvTable)
	}
	L.Push(luaTable)
	return 1
}

func (kvs *KvStoreModule) keys(L *lua.LState) int {
	luaTable := L.NewTable()
	kvResp, err := kvs.kvStore.Keys(L.ToString(1), L.ToString(2), L.ToInt(3))
	if err != nil {
		panic(err)
	}
	for index, kv := range kvResp {
		kvTable := kvToTable(L, kv)
		L.RawSetInt(luaTable, index+1, kvTable)
	}
	L.Push(luaTable)
	return 1
}

func (kvs *KvStoreModule) put(L *lua.LState) int {
	kv, err := kvs.kvStore.Put(L.ToString(1), L.ToString(2), L.ToInt(3))
	if err != nil {
		panic(err)
	}
	L.Push(kvToTable(L, kv))
	return 1
}

func (kvs *KvStoreModule) putjson(L *lua.LState) int {
	kv, err := kvs.kvStore.Put(L.ToString(1), string(luautil.ToJSON(L.CheckAny(2))), L.ToInt(3))
	if err != nil {
		panic(err)
	}
	L.Push(kvToTable(L, kv))
	return 1
}

func (kvs *KvStoreModule) get(L *lua.LState) int {
	kv, err := kvs.kvStore.Get(L.ToString(1), L.ToInt(2))
	if err != nil {
		panic(err)
	}
	L.Push(kvToTable(L, kv))
	return 1
}

func (kvs *KvStoreModule) getjson(L *lua.LState) int {
	kv, err := kvs.kvStore.Get(L.ToString(1), L.ToInt(2))
	if err != nil {
		panic(err)
	}
	table := kvToTable(L, kv)
	L.RawSet(table, lua.LString("value"), luautil.FromJSON(L, []byte(kv.Value)))
	L.Push(table)
	return 1
}
