package gc // import "a4.io/blobstash/pkg/stash/gc"

import (
	"context"

	_ "a4.io/blobstash/pkg/apps/luautil"
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
	return res
}

func (gc *GarbageCollector) GC(ctx context.Context, script string) error {
	if err := gc.L.DoString(script); err != nil {
		return err
	}
	for _, ref := range gc.refs {
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

type blobstore struct {
	dc store.DataContext
}

func newBlobstore(L *lua.LState, dc store.DataContext) (*lua.LUserData, error) {
	bs := &blobstore{dc}
	mt := L.NewTypeMetatable("blobstore")
	L.SetField(mt, "__index", L.SetFuncs(L.NewTable(), map[string]lua.LGFunction{
		"get": blobstoreGet,
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

func blobstoreGet(L *lua.LState) int {
	bs := checkBlobstore(L)
	if bs == nil {
		return 1
	}
	return 0
	// L.Push(buildBody(L, request.body))
	// return 1
}
