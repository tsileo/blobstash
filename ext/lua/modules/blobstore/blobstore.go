package blobstore

import (
	"github.com/tsileo/blobstash/client/interface"
	"github.com/yuin/gopher-lua"
)

type BlobStoreModule struct {
	blobStore client.BlobStorer
}

func New(blobStore client.BlobStorer) *BlobStoreModule {
	return &BlobStoreModule{blobStore}
}

func (bs *BlobStoreModule) Loader(L *lua.LState) int {
	mod := L.SetFuncs(L.NewTable(), map[string]lua.LGFunction{
		"put": bs.put,
		"get": bs.get,
	})
	L.Push(mod)
	return 1
}

func (bs *BlobStoreModule) get(L *lua.LState) int {
	blob, err := bs.blobStore.Get(L.ToString(1))
	luaErr := lua.LString("")
	if err != nil {
		luaErr = lua.LString(err.Error())
	}
	L.Push(lua.LString(string(blob)))
	L.Push(luaErr)
	return 1
}

func (bs *BlobStoreModule) put(L *lua.LState) int {
	if err := bs.blobStore.Put(L.ToString(1), []byte(L.ToString(2))); err != nil {
		L.Push(lua.LString(err.Error()))
	} else {
		L.Push(lua.LString(""))
	}
	return 1
}
