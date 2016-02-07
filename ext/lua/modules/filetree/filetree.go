/*

Package filetree implements a Lua module to interact with the filetree extension.

*/
package filetree

import (
	"fmt"
	"io"
	"mime"
	"net/http"

	"github.com/tsileo/blobstash/embed"
	"github.com/tsileo/blobstash/ext/filetree/filetreeutil/meta"
	"github.com/tsileo/blobstash/ext/filetree/reader/filereader"
	"github.com/yuin/gopher-lua"
)

type FiletreeModule struct {
	blobStore *embed.BlobStore
	r         *http.Request
	w         http.ResponseWriter
}

func New(blobStore *embed.BlobStore, request *http.Request, w http.ResponseWriter) *FiletreeModule {
	return &FiletreeModule{
		blobStore: blobStore,
		r:         request,
		w:         w,
	}
}

func (ft *FiletreeModule) Loader(L *lua.LState) int {
	mod := L.SetFuncs(L.NewTable(), map[string]lua.LGFunction{
		"servefile": ft.servefile,
	})
	L.Push(mod)
	return 1
}

func (ft *FiletreeModule) servefile(L *lua.LState) int {
	ref := L.ToString(1)
	blob, err := ft.blobStore.Get(ref)
	if err != nil {
		panic(err)
	}
	m, err := meta.NewMetaFromBlob(ref, blob)
	if err != nil {
		panic(err)
	}
	f := filereader.NewFile(ft.blobStore, m)
	ft.w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s", m.Name))
	ft.w.Header().Set("Content-Type", mime.TypeByExtension(m.Name))
	if _, err := io.Copy(ft.w, f); err != nil {
		panic(err)
	}
	return 0
}
