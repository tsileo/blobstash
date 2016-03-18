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
	Hash     string                 `json:"-"`
	Children []*Node                `json:"children"`
}

func (ft *FileTreeExt) treeHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		// TODO(tsileo): limit the max depth of the tree
		m := &meta.Meta{}
		httputil.WriteJSON(w, map[string]interface{}{
			"root": m,
		})
	}
}
