package filetree

import (
	"net/http"

	"github.com/gorilla/mux"
	log "gopkg.in/inconshreveable/log15.v2"

	"github.com/tsileo/blobstash/embed"
	"github.com/tsileo/blobstash/ext/filetree/filetreeutil/meta"
	"github.com/tsileo/blobstash/httputil"
	serverMiddleware "github.com/tsileo/blobstash/middleware"
	_ "github.com/tsileo/blobstash/permissions"
	_ "github.com/tsileo/blobstash/vkv"
)

var (
	PermName     = "filetree"
	PermTreeName = "filetree:root:"
	PermWrite    = "write"
	PermRead     = "read"
)

type FileTreeExt struct {
	kvStore   *embed.KvStore
	blobStore *embed.BlobStore

	log log.Logger
}

// New initializes the `DocStoreExt`
func New(logger log.Logger, kvStore *embed.KvStore, blobStore *embed.BlobStore) (*FileTreeExt, error) {
	return &FileTreeExt{
		kvStore:   kvStore,
		blobStore: blobStore,
		log:       logger,
	}, nil
}

// Close closes all the open DB files.
func (ft *FileTreeExt) Close() error {
	return nil
}

// RegisterRoute registers all the HTTP handlers for the extension
func (ft *FileTreeExt) RegisterRoute(r *mux.Router, middlewares *serverMiddleware.SharedMiddleware) {
	ft.log.Debug("RegisterRoute")
	r.Handle("/{ref}", middlewares.Auth(http.HandlerFunc(ft.treeHandler())))
	// r.Handle("/", middlewares.Auth(http.HandlerFunc(docstore.collectionsHandler())))
}

type Node struct {
	Name    string `json:"name"`
	Type    string `json:"type"`
	Size    int    `json:"size"`
	Mode    uint32 `json:"mode"`
	ModTime string `json:"mtime"`
	// Refs    []interface{}          `json:"refs"`
	// Version string                 `json:"version"`
	Extra    map[string]interface{} `json:"extra,omitempty"`
	Hash     string                 `json:"ref"`
	Children []*Node                `json:"children,omitempty"`

	meta *meta.Meta `json:"-"`
}

func metaToNode(m *meta.Meta) (*Node, error) {
	return &Node{
		Name:    m.Name,
		Type:    m.Type,
		Size:    m.Size,
		Mode:    m.Mode,
		ModTime: m.ModTime,
		Extra:   m.Extra,
		Hash:    m.Hash,
		meta:    m,
	}, nil
}

func (ft *FileTreeExt) fetchDir(n *Node, depth int) error {
	if depth >= 10 {
		return nil
	}
	if n.Type == "dir" {
		n.Children = []*Node{}
		for _, ref := range n.meta.Refs {
			blob, err := ft.blobStore.Get(ref.(string))
			if err != nil {
				return err
			}
			m, err := meta.NewMetaFromBlob(n.meta.Hash, blob)
			if err != nil {
				return err
			}
			cn, err := metaToNode(m)
			if err != nil {
				return err
			}
			n.Children = append(n.Children, cn)
			if err := ft.fetchDir(cn, depth+1); err != nil {
				return err
			}
		}
	}
	n.meta.Close()
	return nil
}

func (ft *FileTreeExt) treeHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		// TODO(tsileo): limit the max depth of the tree
		if r.Method != "GET" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		vars := mux.Vars(r)

		hash := vars["ref"]
		blob, err := ft.blobStore.Get(hash)
		if err != nil {
			panic(err)
		}

		m, err := meta.NewMetaFromBlob(hash, blob)
		if err != nil {
			panic(err)
		}
		defer m.Close()

		n, err := metaToNode(m)
		if err != nil {
			panic(err)
		}

		if err := ft.fetchDir(n, 1); err != nil {
			panic(err)
		}

		httputil.WriteJSON(w, map[string]interface{}{
			"root": n,
		})
	}
}
