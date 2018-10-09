package filetree

import (
	"fmt"
	"net/http"
	"os"

	"github.com/dchest/blake2b"
	"github.com/gorilla/mux"
	lua "github.com/yuin/gopher-lua"

	"a4.io/blobstash/pkg/auth"
	"a4.io/blobstash/pkg/client/clientutil"
	"a4.io/blobstash/pkg/ctxutil"
	"a4.io/blobstash/pkg/extra"
	rnode "a4.io/blobstash/pkg/filetree/filetreeutil/node"
	"a4.io/blobstash/pkg/httputil"
	"a4.io/blobstash/pkg/luascripts"
	"a4.io/blobstash/pkg/perms"
)

type searchReq struct {
	Expr         string `json:"expr"`
	Code         string `json:"code"`
	WithContents bool   `json:"with_contents"`
	Limit        int    `json:"limit"`
}

type searchResult struct {
	Path string `json:"path"`
	Node *Node  `json:"node"`
}

func (sr *searchReq) GetCode() string {
	switch {
	case sr.Expr != "":
		return luascripts.Tpl("filetree_expr_search.lua", luascripts.Ctx{"expr": sr.Expr})
	case sr.Code != "":
		return sr.Code
	default:
		// TODO(tsileo): return a 400
		panic("invalid search request")
	}
}

// Fetch a Node outside any FS
func (ft *FileTree) nodeSearchHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		sreq := &searchReq{}
		if err := httputil.Unmarshal(r, sreq); err != nil {
			panic(err)
		}

		L := lua.NewState()
		defer L.Close()
		extra.Setup(L)

		lh, err := newLuaHook(L, sreq.GetCode())
		if err != nil {
			panic(err)
		}

		vars := mux.Vars(r)
		hash := vars["ref"]

		if !auth.Can(
			r,
			perms.Action(perms.Search, perms.Node),
			perms.ResourceWithID(perms.Filetree, perms.Node, hash),
		) {
			auth.Forbidden(w)
			return
		}

		ctx := ctxutil.WithNamespace(r.Context(), r.Header.Get(ctxutil.NamespaceHeader))

		n, err := ft.nodeByRef(ctx, hash)
		if err != nil {
			if err == clientutil.ErrBlobNotFound {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			panic(err)
		}
		if n.Type == "file" {
			panic("cannot search a file")
		}

		result := []*searchResult{}
		// TODO(tsileo): a way to set a limit for search
		if err := ft.IterTree(ctx, n, func(cn *Node, path string) error {
			contents := ""
			if cn.Type == rnode.File && sreq.WithContents {
				// TODO(tsileo): fetch contents if it's a file AND it's not binary data
				contents = ""
			}
			matched, err := lh.Match(convertNode(L, ft, cn), contents)
			if err != nil {
				return err
			}
			if matched {
				result = append(result, &searchResult{Path: path, Node: cn})
			}
			return nil

		}); err != nil {
			panic(err)
		}

		httputil.MarshalAndWrite(r, w, map[string]interface{}{
			"data": result,
			"pagination": map[string]interface{}{
				"cursor":   "",
				"has_more": false,
				"count":    len(result),
				"per_page": len(result),
			},
		})
	}
}

type luaHook struct {
	L        *lua.LState // A pointer of the state from `LuaHooks`
	hookFunc *lua.LFunction
	ID       string
}

func newLuaHook(L *lua.LState, code string) (*luaHook, error) {
	if err := L.DoString(code); err != nil {
		return nil, err
	}
	hookFunc := L.Get(-1).(*lua.LFunction)
	L.Pop(1)
	return &luaHook{
		L:        L,
		hookFunc: hookFunc,
		ID:       fmt.Sprintf("%x", blake2b.Sum256([]byte(code))),
	}, nil
}

func (h *luaHook) Match(dat lua.LValue, contents string) (bool, error) {
	if err := h.L.CallByParam(lua.P{
		Fn:      h.hookFunc,
		NRet:    1,
		Protect: true,
	}, dat, lua.LString(contents)); err != nil {
		fmt.Printf("failed to call pre put hook func: %+v %+v\n", dat, err)
		return false, err
	}
	ret := h.L.Get(-1)
	h.L.Pop(1)
	if ret == lua.LTrue {
		return true, nil
	}
	return false, nil
}

func convertNode(L *lua.LState, ft *FileTree, node *Node) *lua.LTable {
	tbl := L.CreateTable(0, 7)
	tbl.RawSetH(lua.LString("hash"), lua.LString(node.Hash))
	tbl.RawSetH(lua.LString("name"), lua.LString(node.Name))
	tbl.RawSetH(lua.LString("type"), lua.LString(node.Type))
	tbl.RawSetH(lua.LString("mtime"), lua.LString(node.ModTime))
	tbl.RawSetH(lua.LString("citme"), lua.LString(node.ChangeTime))
	tbl.RawSetH(lua.LString("mode"), lua.LNumber(os.FileMode(node.Mode)))
	tbl.RawSetH(lua.LString("size"), lua.LNumber(node.Size))
	return tbl
}
