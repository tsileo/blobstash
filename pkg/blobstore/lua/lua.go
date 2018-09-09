package lua // import "a4.io/blobstash/pkg/blobstore/lua"

import (
	"context"
	"fmt"

	humanize "github.com/dustin/go-humanize"
	"github.com/yuin/gopher-lua"

	"a4.io/blobstash/pkg/blob"
	"a4.io/blobstash/pkg/stash/store"
)

func convertBlobRef(L *lua.LState, br *blob.SizedBlobRef) *lua.LTable {
	tbl := L.CreateTable(0, 3)
	tbl.RawSetH(lua.LString("hash"), lua.LString(br.Hash))
	tbl.RawSetH(lua.LString("size"), lua.LNumber(br.Size))
	tbl.RawSetH(lua.LString("size_human"), lua.LString(humanize.Bytes(uint64(br.Size))))

	return tbl
}

func setupBlobStore(ctx context.Context, L *lua.LState, bs store.BlobStore) func(*lua.LState) int {
	return func(L *lua.LState) int {
		// register functions to the table
		mod := L.SetFuncs(L.NewTable(), map[string]lua.LGFunction{
			"blobs": func(L *lua.LState) int {
				startCursor := L.ToString(1)
				blobs, cursor, err := bs.Enumerate(context.TODO(), startCursor, startCursor+"\xff", 0)
				if err != nil {
					panic(err)
				}
				blobsTbl := L.CreateTable(len(blobs), 0)
				for _, blob := range blobs {
					blobsTbl.Append(convertBlobRef(L, blob))
				}
				L.Push(blobsTbl)
				L.Push(lua.LString(cursor))
				return 2
			},
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

func Setup(ctx context.Context, L *lua.LState, bs store.BlobStore) {
	L.PreloadModule("blobstore", setupBlobStore(ctx, L, bs))
}
