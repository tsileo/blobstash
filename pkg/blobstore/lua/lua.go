package lua // import "a4.io/blobstash/pkg/blobstore/lua"

import (
	"context"
	"fmt"

	"github.com/yuin/gopher-lua"

	"a4.io/blobstash/pkg/stash/store"
)

func setupBlobStore(L *lua.LState, bs store.BlobStore, ctx context.Context) func(*lua.LState) int {
	return func(L *lua.LState) int {
		// register functions to the table
		mod := L.SetFuncs(L.NewTable(), map[string]lua.LGFunction{
			"stat": func(L *lua.LState) int {
				data, err := bs.Stat(ctx, L.ToString(1))
				if err != nil {
					L.Push(lua.LNil)
					return 1
					// FIXME(tsileo): panic here? ensure data can be false
				}
				if data == true {
					L.Push(lua.LTrue)
				} else {
					L.Push(lua.LFalse)
				}
				return 1
			},
			"get": func(L *lua.LState) int {
				data, err := bs.Get(ctx, L.ToString(1))
				if err != nil {
					fmt.Printf("failed to fetch %s: %v\n", L.ToString(1), err)
					L.Push(lua.LNil)
					return 1
					// TODO(tsileo): handle not found
				}
				L.Push(lua.LString(data))
				return 1
			},
		})
		// returns the module
		L.Push(mod)
		return 1
	}
}

func Setup(L *lua.LState, bs store.BlobStore, ctx context.Context) {
	L.PreloadModule("blobstore", setupBlobStore(L, bs, ctx))
}
