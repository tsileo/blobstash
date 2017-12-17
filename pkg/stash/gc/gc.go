package gc // import "a4.io/blobstash/pkg/stash/gc"

import (
	"context"
	"fmt"

	"github.com/vmihailenco/msgpack"
	"github.com/yuin/gopher-lua"

	"a4.io/blobstash/pkg/apps/luautil"
	"a4.io/blobstash/pkg/blob"
	"a4.io/blobstash/pkg/stash"
	"a4.io/blobstash/pkg/stash/store"
)

type GarbageCollector struct {
	dataContext store.DataContext
	stash       *stash.Stash
	L           *lua.LState
	refs        map[string]struct{}
}

func New(s *stash.Stash, dc store.DataContext) *GarbageCollector {
	L := lua.NewState()
	res := &GarbageCollector{
		L:           L,
		dataContext: dc,
		refs:        map[string]struct{}{},
		stash:       s,
	}

	// mark(<blob hash>) is the lowest-level func, it "mark"s a blob to be copied to the root blobstore
	mark := func(L *lua.LState) int {
		ref := L.ToString(1)
		if _, ok := res.refs[ref]; !ok {
			res.refs[ref] = struct{}{}
		}
		return 0
	}
	L.SetGlobal("mark", L.NewFunction(mark))
	L.PreloadModule("json", loadJSON)
	L.PreloadModule("msgpack", loadMsgpack)
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
	if err := L.DoString(`
local msgpack = require('msgpack')
function mark_kv (key, version)
  print(key)
  print(version)
  local h = blobstash.kvstore:get_meta_blob(key, version)
  print(h)
  if h ~= nil then
    mark(h)
    local _, ref = blobstash.kvstore:get(key, version)
    if ref ~= '' then
      mark(ref)
    end
  end
end
_G.mark_kv = mark_kv
function mark_filetree_node (ref)
  local data = blobstash.blobstore:get(ref)
  local node = msgpack.decode(data)
  mark(ref)
  if node.t == 'dir' then
    for _, childRef in ipairs(node.r) do
      mark_filetree_node(childRef)
    end
  else
    for _, contentRef in ipairs(node.r) do
      mark(contentRef[2])
    end
  end
end
_G.mark_filetree_node = mark_filetree_node
`); err != nil {
		panic(err)
	}
	// FIXME(tsileo): do like in the docstore, export code _G.mark_kv(key, version), _G.mark_fs_ref(ref)...
	// and the option to load custom GC script from the filesystem like stored queries
	return res
}

func (gc *GarbageCollector) GC(ctx context.Context, script string) error {
	if err := gc.L.DoString(script); err != nil {
		return err
	}
	fmt.Printf("refs=%+v\n", gc.refs)
	for ref, _ := range gc.refs {
		// FIXME(tsileo): stat before get/put

		data, err := gc.dataContext.BlobStoreProxy().Get(ctx, ref)
		if err != nil {
			return err
		}

		if err := gc.stash.Root().BlobStore().Put(ctx, &blob.Blob{Hash: ref, Data: data}); err != nil {
			return err
		}
	}
	// return nil
	// FIXME(tsileo): make destroying the context optional
	return gc.dataContext.Destroy()
}

func loadMsgpack(L *lua.LState) int {
	// register functions to the table
	mod := L.SetFuncs(L.NewTable(), map[string]lua.LGFunction{
		"decode": msgpackDecode,
		"encode": msgpackEncode,
	})
	// returns the module
	L.Push(mod)
	return 1
}

func msgpackEncode(L *lua.LState) int {
	data := L.CheckAny(1)
	if data == nil {
		L.Push(lua.LNil)
		return 1
	}
	txt, err := msgpack.Marshal(data)
	if err != nil {
		panic(err)
	}
	L.Push(lua.LString(string(txt)))
	return 1
}

// TODO(tsileo): a note about empty list vs empty object
func msgpackDecode(L *lua.LState) int {
	data := L.ToString(1)
	out := map[string]interface{}{}
	if err := msgpack.Unmarshal([]byte(data), &out); err != nil {
		panic(err)
	}
	L.Push(luautil.InterfaceToLValue(L, out))
	return 1
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
	data, err := bs.dc.BlobStoreProxy().Stat(context.TODO(), L.ToString(2))
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
	data, err := bs.dc.BlobStoreProxy().Get(context.TODO(), L.ToString(2))
	if err != nil {
		fmt.Printf("failed to fetch %s: %v\n", L.ToString(2), err)
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
	version, err := strconv.Atoi(L.ToString(3))
	if err != nil {
		L.ArgError(3, "version must be a valid int")
		return
	}
	fkv, err := kv.dc.KvStoreProxy().Get(context.TODO(), L.ToString(2), version)
	if err != nil {
		panic(err)
	}
	L.Push(lua.LString(fkv.Data))
	L.Push(lua.LString(fkv.HexHash()))
	L.Push(lua.LString(strconv.Itoa(fkv.version)))
	// FIXME(tsileo): fix the mark_* script
	return 3
}

func kvstoreGetMetaBlob(L *lua.LState) int {
	kv := checkKvstore(L)
	if kv == nil {
		return 1
	}
	version, err := strconv.Atoi(L.ToString(3))
	if err != nil {
		L.ArgError(3, "version must be a valid int")
		return
	}
	data, err := kv.dc.KvStoreProxy().GetMetaBlob(context.TODO(), L.ToString(2), version)
	if err != nil {
		L.Push(lua.LNil)
		return 1
		// TODO(tsileo): handle not found
	}
	L.Push(lua.LString(data))
	return 1
}
