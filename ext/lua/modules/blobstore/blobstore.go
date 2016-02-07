package blobstore

import (
	"time"

	"github.com/tsileo/blobstash/embed"
	"github.com/tsileo/blobstash/ext/lua/luautil"
	"github.com/yuin/gopher-lua"
)

type BlobStoreModule struct {
	blobStore *embed.BlobStore
}

func New(blobStore *embed.BlobStore) *BlobStoreModule {
	return &BlobStoreModule{blobStore}
}

func (bs *BlobStoreModule) Loader(L *lua.LState) int {
	mod := L.SetFuncs(L.NewTable(), map[string]lua.LGFunction{
		"put":     bs.put,
		"get":     bs.get,
		"getjson": bs.getjson,
		"stat":    bs.stat,
		// "enumerate": bs.enumerate,
	})
	L.Push(mod)
	return 1
}

func (bs *BlobStoreModule) enumerate(L *lua.LState) int {
	// FIXME(tsileo) enumerate seems to be broken
	cblobs := make(chan string)
	errc := make(chan error)
	luaTable := L.NewTable()
	go func() {
		errc <- bs.blobStore.Enumerate(cblobs, L.ToString(1), L.ToString(2), L.ToInt(3))
	}()
	index := 1
	for blobHash := range cblobs {
		L.RawSetInt(luaTable, index, lua.LString(blobHash))
		index++
	}
	if err := <-errc; err != nil {
		panic(err)
	}
	L.Push(luaTable)
	return 1
}

func (bs *BlobStoreModule) stat(L *lua.LState) int {
	exists, err := bs.blobStore.Stat(L.ToString(1))
	if err != nil {
		panic(err)
	}
	L.Push(lua.LBool(exists))
	return 1
}

func (bs *BlobStoreModule) get(L *lua.LState) int {
	blob, err := bs.blobStore.Get(L.ToString(1))
	if err != nil {
		panic(err)
	}
	// FIXME(tsileo) handle blob not found
	L.Push(lua.LString(string(blob)))
	return 1
}

func (bs *BlobStoreModule) getjson(L *lua.LState) int {
	blob, err := bs.blobStore.Get(L.ToString(1))
	if err != nil {
		panic(err)
	}
	// FIXME(tsileo) handle blob not found
	L.Push(luautil.FromJSON(L, blob))
	return 1
}

func (bs *BlobStoreModule) put(L *lua.LState) int {
	if err := bs.blobStore.Put(L.ToString(1), []byte(L.ToString(2)), ""); err != nil {
		panic(err)
	}
	// Since blob write are async, the sleep ensure a get right after the put retrieve the blob
	time.Sleep(5 * time.Millisecond)
	return 0
}
