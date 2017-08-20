package gc // import "a4.io/blobstash/pkg/stash/gc"

import (
	"context"

	"a4.io/blobstash/pkg/apps/luautil"
	"a4.io/blobstash/pkg/blob"
	"a4.io/blobstash/pkg/stash"
	"a4.io/blobstash/pkg/stash/store"
	"github.com/yuin/gopher-lua"
)

// XXX(tsileo): take store interface, and exec store Lua script that can
// read a blob, or a key/version(/iterate keys?) and
// can get the blob for a kv(key, version) and "mark" blob for GC

type GarbageCollector struct {
	dataContext store.DataContext
	stash       *stash.Stash
	L           *lua.LState
	refs        []string
}

func New(s *stash.Stash, dc store.DataContext) *GarbageCollector {
	L := lua.NewState()
	res := &GarbageCollector{
		L:           L,
		dataContext: dc,
		refs:        []string{},
		stash:       s,
	}

	// mark(<blob hash>) is the lowest-level func, it "mark"s a blob to be copied to the root blobstore
	mark := func(L *lua.LState) int {
		ref := L.ToString(1)
		res.refs = append(res.refs, ref)
		return 0
	}
	L.SetGlobal("mark", L.NewFunction(mark))
	L.PreloadModule("json", loadJSON)
	bs, err := newBlobstore(L, dc)
	if err != nil {
		panic(err)
	}
	kvs, err := newKvstore(L, dc)
	if err != nil {
		panic(err)
	}
	rootTable := L.CreateTable(0, 2)
	rootTable.RawSetH(lua.LString("blobstore"), bs)
	rootTable.RawSetH(lua.LString("kvstore"), kvs)
	L.SetGlobal("blobstash", rootTable)
	return res
}

func (gc *GarbageCollector) GC(ctx context.Context, script string) error {
	if err := gc.L.DoString(script); err != nil {
		return err
	}
	for _, ref := range gc.refs {
		// FIXME(tsileo): stat before get/put

		data, err := gc.dataContext.BlobStore().Get(ctx, ref)
		if err != nil {
			return err
		}

		if err := gc.stash.Root().BlobStore().Put(ctx, &blob.Blob{Hash: ref, Data: data}); err != nil {
			return err
		}
	}
	return gc.dataContext.Destroy()
}

func loadJSON(L *lua.LState) int {
	// register functions to the table
	mod := L.SetFuncs(L.NewTable(), map[string]lua.LGFunction{
		"decode": jsonDecode,
		"encode": jsonEncode,
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
	L.Push(lua.LString(string(luautil.ToJSON(data))))
	return 1
}

// TODO(tsileo): a note about empty list vs empty object
func jsonDecode(L *lua.LState) int {
	data := L.ToString(1)
	L.Push(luautil.FromJSON(L, []byte(data)))
	return 1
}

type blobstore struct {
	dc store.DataContext
}

func newBlobstore(L *lua.LState, dc store.DataContext) (*lua.LUserData, error) {
	bs := &blobstore{dc}
	mt := L.NewTypeMetatable("blobstore")
	L.SetField(mt, "__index", L.SetFuncs(L.NewTable(), map[string]lua.LGFunction{
		"get":  blobstoreGet,
		"stat": blobstoreStat,
	}))
	ud := L.NewUserData()
	ud.Value = bs
	L.SetMetatable(ud, L.GetTypeMetatable("blobstore"))
	return ud, nil
}

func checkBlobstore(L *lua.LState) *blobstore {
	ud := L.CheckUserData(1)
	if v, ok := ud.Value.(*blobstore); ok {
		return v
	}
	L.ArgError(1, "blobstore expected")
	return nil
}

func blobstoreStat(L *lua.LState) int {
	bs := checkBlobstore(L)
	if bs == nil {
		return 1
	}
	data, err := bs.dc.BlobStore().Stat(context.TODO(), L.ToString(2))
	if err != nil {
		L.Push(lua.LNil)
		return 1
		// TODO(tsileo): handle not found
	}
	if data == true {
		L.Push(lua.LTrue)
	} else {
		L.Push(lua.LFalse)
	}
	return 1
}

func blobstoreGet(L *lua.LState) int {
	bs := checkBlobstore(L)
	if bs == nil {
		return 1
	}
	data, err := bs.dc.BlobStore().Get(context.TODO(), L.ToString(2))
	if err != nil {
		L.Push(lua.LNil)
		return 1
		// TODO(tsileo): handle not found
	}
	L.Push(lua.LString(data))
	return 1
	// L.Push(buildBody(L, request.body))
	// return 1
}

type kvstore struct {
	dc store.DataContext
}

func newKvstore(L *lua.LState, dc store.DataContext) (*lua.LUserData, error) {
	kvs := &kvstore{dc}
	mt := L.NewTypeMetatable("kvstore")
	L.SetField(mt, "__index", L.SetFuncs(L.NewTable(), map[string]lua.LGFunction{
		"get_meta_blob": kvstoreGetMetaBlob,
		"get":           kvstoreGet,
	}))
	ud := L.NewUserData()
	ud.Value = kvs
	L.SetMetatable(ud, L.GetTypeMetatable("kvstore"))
	return ud, nil
}

func checkKvstore(L *lua.LState) *kvstore {
	ud := L.CheckUserData(1)
	if v, ok := ud.Value.(*kvstore); ok {
		return v
	}
	L.ArgError(1, "kvstore expected")
	return nil
}

func kvstoreGet(L *lua.LState) int {
	kv := checkKvstore(L)
	if kv == nil {
		return 1
	}
	fkv, err := kv.dc.KvStore().Get(context.TODO(), L.ToString(2), L.ToInt(3))
	if err != nil {
		panic(err)
	}
	L.Push(lua.LString(fkv.Data))
	L.Push(lua.LString(fkv.HexHash()))
	return 2
}

func kvstoreGetMetaBlob(L *lua.LState) int {
	kv := checkKvstore(L)
	if kv == nil {
		return 1
	}
	data, err := kv.dc.KvStore().GetMetaBlob(context.TODO(), L.ToString(2), L.ToInt(3))
	if err != nil {
		L.Push(lua.LNil)
		return 1
		// TODO(tsileo): handle not found
	}
	L.Push(lua.LString(data))
	return 1
}
