package filetree

import (
	"github.com/gorilla/mux"
	log "gopkg.in/inconshreveable/log15.v2"

	"github.com/tsileo/blobstash/embed"
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

	logger log.Logger
}

// New initializes the `DocStoreExt`
func New(logger log.Logger, kvStore *embed.KvStore, blobStore *embed.BlobStore) (*FileTreeExt, error) {
	return &FileTreeExt{
		kvStore:   kvStore,
		blobStore: blobStore,
		logger:    logger,
	}, nil
}

// Close closes all the open DB files.
func (ft *FileTreeExt) Close() error {
	return nil
}

// RegisterRoute registers all the HTTP handlers for the extension
func (ft *FileTreeExt) RegisterRoute(r *mux.Router, middlewares *serverMiddleware.SharedMiddleware) {
	// r.Handle("/", middlewares.Auth(http.HandlerFunc(docstore.collectionsHandler())))
}
