package lua // import "a4.io/blobstash/pkg/filetree/lua"

import (
	"context"
	"os"

	humanize "github.com/dustin/go-humanize"
	"github.com/phayes/permbits"
	"github.com/yuin/gopher-lua"

	"a4.io/blobstash/pkg/filetree"
	"a4.io/blobstash/pkg/filetree/writer"
	"a4.io/blobstash/pkg/stash/store"
)

func buildFSInfo(L *lua.LState, name, ref string) *lua.LTable {
	tbl := L.CreateTable(0, 2)
	tbl.RawSetH(lua.LString("name"), lua.LString(name))
	tbl.RawSetH(lua.LString("ref"), lua.LString(ref))
	return tbl
}

func convertNode(L *lua.LState, ft *filetree.FileTree, node *filetree.Node) *lua.LTable {
	tbl := L.CreateTable(0, 10)
	dlURL, embedURL, err := ft.GetSemiPrivateLink(node)
	if err != nil {
		panic(err)
	}
	tbl.RawSetH(lua.LString("url"), lua.LString(embedURL))
	tbl.RawSetH(lua.LString("dl_url"), lua.LString(dlURL))

	tbl.RawSetH(lua.LString("hash"), lua.LString(node.Hash))
	tbl.RawSetH(lua.LString("name"), lua.LString(node.Name))
	tbl.RawSetH(lua.LString("type"), lua.LString(node.Type))
	tbl.RawSetH(lua.LString("mtime"), lua.LString(node.ModTime))
	tbl.RawSetH(lua.LString("citme"), lua.LString(node.ChangeTime))
	tbl.RawSetH(lua.LString("mode"), lua.LString(permbits.FileMode(os.FileMode(node.Mode)).String()))
	tbl.RawSetH(lua.LString("size"), lua.LNumber(node.Size))
	tbl.RawSetH(lua.LString("size_human"), lua.LString(humanize.Bytes(uint64(node.Size))))
	childrenTbl := L.CreateTable(len(node.Children), 0)
	for _, child := range node.Children {
		childrenTbl.Append(convertNode(L, ft, child))
	}
	tbl.RawSetH(lua.LString("children"), childrenTbl)
	return tbl
}

func setupFileTree(ft *filetree.FileTree, bs store.BlobStore) func(*lua.LState) int {
	return func(L *lua.LState) int {
		// register functions to the table
		mod := L.SetFuncs(L.NewTable(), map[string]lua.LGFunction{
			"iter_fs": func(L *lua.LState) int {
				it, err := ft.IterFS(context.TODO(), "")
				if err != nil {
					panic(err)
				}
				tbl := L.CreateTable(len(it), 0)
				for _, kv := range it {
					tbl.Append(buildFSInfo(L, kv.Name, kv.Ref))
				}
				L.Push(tbl)
				return 1
			},
			"fs": func(L *lua.LState) int {
				fs := filetree.NewFS(L.ToString(1), ft)
				node, _, _, err := fs.Path(context.TODO(), "/", 1, false, 0)
				if err != nil {
					panic(err)
				}
				L.Push(convertNode(L, ft, node))
				return 1
			},
			"node": func(L *lua.LState) int {
				node, err := ft.NodeWithChildren(context.TODO(), L.ToString(2))
				if err != nil {
					panic(err)
				}
				path, err := ft.BruteforcePath(context.TODO(), L.ToString(1), L.ToString(2))
				if err != nil {
					panic(err)
				}
				pathTable := L.CreateTable(len(path), 0)
				for _, nodeInfo := range path {
					pathTable.Append(buildFSInfo(L, nodeInfo.Name, nodeInfo.Ref))
				}
				L.Push(convertNode(L, ft, node))
				L.Push(pathTable)
				return 2
			},
			"put_file": func(L *lua.LState) int {
				uploader := writer.NewUploader(filetree.NewBlobStoreCompat(bs, context.TODO()))
				name := L.ToString(1)
				newName := L.ToString(2)
				extraMeta := L.ToBool(3)
				var ref string
				if newName != "" {
					// Upload the given file with a new name and without meta data (mtime/ctime/mode)
					node, err := uploader.PutFileRename(name, newName, extraMeta)
					if err != nil {
						panic(err)
					}
					ref = node.Hash
				} else {
					node, err := uploader.PutFile(name)
					if err != nil {
						panic(err)
					}
					ref = node.Hash
				}
				L.Push(lua.LString(ref))
				return 1
			},
		})
		// returns the module
		L.Push(mod)
		return 1
	}
}

// Setup loads the filetree Lua module
func Setup(L *lua.LState, ft *filetree.FileTree, bs store.BlobStore) {
	L.PreloadModule("filetree", setupFileTree(ft, bs))
}
