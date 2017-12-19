package lua // import "a4.io/blobstash/pkg/filetree/lua"

import (
	"context"

	"github.com/yuin/gopher-lua"

	_ "a4.io/blobstash/pkg/blobstore"
	"a4.io/blobstash/pkg/filetree"
	"a4.io/blobstash/pkg/filetree/writer"
	"a4.io/blobstash/pkg/stash/store"
)

func setupFileTree(ft *filetree.FileTree, bs store.BlobStore) func(*lua.LState) int {
	return func(L *lua.LState) int {
		// register functions to the table
		mod := L.SetFuncs(L.NewTable(), map[string]lua.LGFunction{
			"put_file": func(L *lua.LState) int {
				uploader := writer.NewUploader(filetree.NewBlobStoreCompat(bs, context.TODO()))
				name := L.ToString(1)
				newName := L.ToString(2)
				var ref string
				if newName != "" {
					node, err := uploader.PutAndRenameFile(name, newName)
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

func Setup(L *lua.LState, ft *filetree.FileTree, bs store.BlobStore) {
	L.PreloadModule("filetree", setupFileTree(ft, bs))
}
