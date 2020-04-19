package gc // import "a4.io/blobstash/pkg/stash/gc"

import (
	"context"
	"fmt"

	"github.com/vmihailenco/msgpack"
	lua "github.com/yuin/gopher-lua"

	"a4.io/blobsfile"
	"a4.io/blobstash/pkg/apps/luautil"
	"a4.io/blobstash/pkg/blob"
	bsLua "a4.io/blobstash/pkg/blobstore/lua"
	"a4.io/blobstash/pkg/extra"
	"a4.io/blobstash/pkg/filetree/filetreeutil/node"
	"a4.io/blobstash/pkg/hub"
	kvsLua "a4.io/blobstash/pkg/kvstore/lua"
	"a4.io/blobstash/pkg/luascripts"
	"a4.io/blobstash/pkg/stash"
	"a4.io/blobstash/pkg/stash/store"
)

func GC(ctx context.Context, h *hub.Hub, s *stash.Stash, dc store.DataContext, script string, existingRefs map[string]struct{}) (int, uint64, error) {

	// TODO(tsileo): take a logger
	refs := map[string]struct{}{}
	orderedRefs := []string{}

	L := lua.NewState()
	defer L.Close()
	var skipped int

	// premark(<blob hash>) notify the GC that this blob is already in the root blobstore explicitely (to speedup huge GC)
	premark := func(L *lua.LState) int {
		// TODO(tsileo): debug logging here to help troubleshot GC issues
		ref := L.ToString(1)
		if _, ok := existingRefs[ref]; !ok {
			existingRefs[ref] = struct{}{}
		}
		return 0
	}

	// mark(<blob hash>) is the lowest-level func, it "mark"s a blob to be copied to the root blobstore
	mark := func(L *lua.LState) int {
		// TODO(tsileo): debug logging here to help troubleshot GC issues
		ref := L.ToString(1)
		// if _, ok := existingRefs[ref]; ok {
		//	skipped++
		//	return 0
		// }
		if _, ok := refs[ref]; !ok {
			refs[ref] = struct{}{}
			orderedRefs = append(orderedRefs, ref)
		}
		return 0
	}

	L.SetGlobal("premark", L.NewFunction(premark))
	L.SetGlobal("mark", L.NewFunction(mark))
	L.PreloadModule("json", loadJSON)
	L.PreloadModule("msgpack", loadMsgpack)
	L.PreloadModule("node", loadNode)
	kvsLua.Setup(L, s.KvStore(), ctx)
	bsLua.Setup(ctx, L, s.BlobStore())
	extra.Setup(L)

	// Setup two global:
	// - mark_kv(key, version)  -- version must be a String because we use nano ts
	// - mark_filetree_node(ref)
	if err := L.DoString(luascripts.Get("stash_gc.lua")); err != nil {
		return 0, 0, err
	}

	if err := L.DoString(script); err != nil {
		return 0, 0, err
	}
	fmt.Printf("refs=%q\n", orderedRefs)
	blobsCnt := 0
	totalSize := uint64(0)
	for _, ref := range orderedRefs {
		// FIXME(tsileo): stat before get/put

		// Get the marked blob from the blobstore proxy
		data, err := dc.StashBlobStore().Get(ctx, ref)
		if err != nil {
			if err == blobsfile.ErrBlobNotFound {
				continue
			}
			return 0, 0, err
		}

		// Save it in the root blobstore
		saved, err := s.Root().BlobStore().Put(ctx, &blob.Blob{Hash: ref, Data: data})
		if err != nil {
			return 0, 0, err
		}

		if saved {
			blobsCnt++
			totalSize += uint64(len(data))
		}
	}
	fmt.Printf("premarking helped skipped %d blobs, refs=%d blobs, saved %d blobs\n", skipped, len(orderedRefs), blobsCnt)

	return blobsCnt, totalSize, nil
}

// FIXME(tsileo): have a single share "Lua lib" for all the Lua interactions (GC, document store...)
func loadNode(L *lua.LState) int {
	// register functions to the table
	mod := L.SetFuncs(L.NewTable(), map[string]lua.LGFunction{
		"decode": nodeDecode,
	})
	// returns the module
	L.Push(mod)
	return 1
}

// TODO(tsileo): a note about empty list vs empty object
func nodeDecode(L *lua.LState) int {
	data := L.ToString(1)
	blob := []byte(data)
	if encoded, ok := node.IsNodeBlob(blob); ok {
		blob = encoded
	}
	out := map[string]interface{}{}
	if err := msgpack.Unmarshal(blob, &out); err != nil {
		panic(err)
	}
	L.Push(luautil.InterfaceToLValue(L, out))
	return 1
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
	L.Push(lua.LString(string(luautil.ToJSON(L, data))))
	return 1
}

// TODO(tsileo): a note about empty list vs empty object
func jsonDecode(L *lua.LState) int {
	data := L.ToString(1)
	L.Push(luautil.FromJSON(L, []byte(data)))
	return 1
}
