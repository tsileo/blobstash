package lua // import "a4.io/blobstash/pkg/kvstore/lua"

import (
	"context"
	"strconv"
	"time"

	"github.com/yuin/gopher-lua"

	"a4.io/blobstash/pkg/stash/store"
	"a4.io/blobstash/pkg/vkv"
)

func convertKv(L *lua.LState, kv *vkv.KeyValue) *lua.LTable {
	tbl := L.CreateTable(0, 5)
	tbl.RawSetH(lua.LString("key"), lua.LString(kv.Key))
	tbl.RawSetH(lua.LString("version"), lua.LString(strconv.FormatInt(kv.Version, 10)))
	tbl.RawSetH(lua.LString("version_human"), lua.LString(time.Unix(0, kv.Version).Format(time.RFC3339)))
	tbl.RawSetH(lua.LString("ref"), lua.LString(kv.HexHash()))
	tbl.RawSetH(lua.LString("data"), lua.LString(kv.Data))
	return tbl
}

func setupKvStore(L *lua.LState, kvs store.KvStore, ctx context.Context) func(*lua.LState) int {
	return func(L *lua.LState) int {
		// register functions to the table
		mod := L.SetFuncs(L.NewTable(), map[string]lua.LGFunction{
			"keys": func(L *lua.LState) int {
				keys, cursor, err := kvs.Keys(context.TODO(), L.ToString(1), "\xff", 100)
				if err != nil {
					panic(err)
				}
				tbl := L.CreateTable(len(keys), 0)
				for _, kv := range keys {
					tbl.Append(convertKv(L, kv))
				}
				L.Push(tbl)
				L.Push(lua.LString(cursor))
				return 2
			},
			"get_meta_blob": func(L *lua.LState) int {
				version, err := strconv.ParseInt(L.ToString(2), 10, 0)
				if err != nil {
					L.ArgError(2, "version must be a valid int")
					return 0
				}
				data, err := kvs.GetMetaBlob(ctx, L.ToString(1), version)
				if err != nil {
					L.Push(lua.LNil)
					return 1
					// TODO(tsileo): handle not found
				}
				L.Push(lua.LString(data))
				return 1
			},
			"key": func(L *lua.LState) int {
				version, err := strconv.ParseInt(L.ToString(2), 10, 0)
				if err != nil {
					L.ArgError(2, "version must be a valid int")
					return 0
				}
				fkv, err := kvs.Get(ctx, L.ToString(1), version)
				if err != nil {
					panic(err)
				}
				L.Push(convertKv(L, fkv))
				return 1
				// L.Push(lua.LString(fkv.Data))
				// L.Push(lua.LString(fkv.HexHash()))
				// L.Push(lua.LString(strconv.FormatInt(fkv.Version, 10)))
				// return 3

			},

			"get": func(L *lua.LState) int {
				version, err := strconv.ParseInt(L.ToString(2), 10, 0)
				if err != nil {
					L.ArgError(2, "version must be a valid int")
					return 0
				}
				fkv, err := kvs.Get(ctx, L.ToString(1), version)
				if err != nil {
					panic(err)
				}
				L.Push(lua.LString(fkv.Data))
				L.Push(lua.LString(fkv.HexHash()))
				L.Push(lua.LString(strconv.FormatInt(fkv.Version, 10)))
				return 3

			},
		})
		// returns the module
		L.Push(mod)
		return 1
	}
}

func Setup(L *lua.LState, kvs store.KvStore, ctx context.Context) {
	L.PreloadModule("kvstore", setupKvStore(L, kvs, ctx))
}
