package lua // import "a4.io/blobstash/pkg/kvstore/lua"

import (
	"context"
	"strconv"

	"github.com/yuin/gopher-lua"

	"a4.io/blobstash/pkg/stash/store"
)

func setupKvStore(L *lua.LState, kvs store.KvStore, ctx context.Context) func(*lua.LState) int {
	return func(L *lua.LState) int {
		// register functions to the table
		mod := L.SetFuncs(L.NewTable(), map[string]lua.LGFunction{
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
