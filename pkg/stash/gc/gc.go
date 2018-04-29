package gc // import "a4.io/blobstash/pkg/stash/gc"

import (
	"context"
	"fmt"

	"github.com/vmihailenco/msgpack"
	"github.com/yuin/gopher-lua"

	"a4.io/blobstash/pkg/apps/luautil"
	"a4.io/blobstash/pkg/blob"
	bsLua "a4.io/blobstash/pkg/blobstore/lua"
	"a4.io/blobstash/pkg/hub"
	kvsLua "a4.io/blobstash/pkg/kvstore/lua"
	"a4.io/blobstash/pkg/stash"
)

func GC(ctx context.Context, h *hub.Hub, s *stash.Stash, script string, remoteRefs map[string]string) error {

	// TODO(tsileo): take a logger
	refs := map[string]struct{}{}

	L := lua.NewState()

	// mark(<blob hash>) is the lowest-level func, it "mark"s a blob to be copied to the root blobstore
	mark := func(L *lua.LState) int {
		// TODO(tsileo): debug logging here to help troubleshot GC issues
		ref := L.ToString(1)
		if _, ok := refs[ref]; !ok {
			refs[ref] = struct{}{}
		}
		return 0
	}

	L.SetGlobal("mark", L.NewFunction(mark))
	L.PreloadModule("json", loadJSON)
	L.PreloadModule("msgpack", loadMsgpack)
	kvsLua.Setup(L, s.KvStore(), ctx)
	bsLua.Setup(L, s.BlobStore(), ctx)

	if err := L.DoString(`
local msgpack = require('msgpack')
local kvstore = require('kvstore')
local blobstore = require('blobstore')

function mark_kv (key, version)
  local h = kvstore.get_meta_blob(key, version)
  if h ~= nil then
    mark(h)
    local _, ref, _ = kvstore.get(key, version)
    if ref ~= '' then
      mark(ref)
    end
  end
end
_G.mark_kv = mark_kv

function mark_filetree_node (ref)
  local data = blobstore.get(ref)
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
		return err
	}
	// FIXME(tsileo): do like in the docstore, export code _G.mark_kv(key, version), _G.mark_fs_ref(ref)...
	// and the option to load custom GC script from the filesystem like stored queries

	if err := L.DoString(script); err != nil {
		return err
	}
	fmt.Printf("Remote refs=%+v\n\nrefs=%+v\n\n", remoteRefs, refs)
	for ref, _ := range refs {
		// FIXME(tsileo): stat before get/put

		// If there's a remote ref available, trigger an "async" remote sync
		if remoteRefs != nil {
			if remoteRef, ok := remoteRefs[ref]; ok {
				fmt.Printf("sync remote\n\n")
				if err := h.NewSyncRemoteBlobEvent(ctx, &blob.Blob{Hash: ref, Extra: remoteRef}, nil); err != nil {
					return err
				}
				delete(remoteRefs, ref)
				continue
			}
		}

		// Get the marked blob from the blobstore proxy
		data, err := s.BlobStore().Get(ctx, ref)
		if err != nil {
			return err
		}

		// Save it in the root blobstore
		if err := s.Root().BlobStore().Put(ctx, &blob.Blob{Hash: ref, Data: data}); err != nil {
			return err
		}
	}

	fmt.Printf("Remote refs=%+v\n\nrefs=%+v\n\n", remoteRefs, refs)
	// Delete the remaining S3 objects that haven't been marked for sync
	// XXX(tsileo): do this after the DoAndDestroy in the parent func?
	for _, remoteRef := range remoteRefs {
		if err := h.NewDeleteRemoteBlobEvent(ctx, nil, remoteRef); err != nil {
			return err
		}
	}
	return nil
}

// FIXME(tsileo): have a single share "Lua lib" for all the Lua interactions (GC, document store...)
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
