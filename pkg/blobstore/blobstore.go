package blobstore

import (
	"golang.org/x/net/context"

	_ "github.com/tsileo/blobstash/backend"
	"github.com/tsileo/blobstash/config"
	"github.com/tsileo/blobstash/config/pathutil"
	"github.com/tsileo/blobstash/pkg/blob"
	"github.com/tsileo/blobstash/pkg/router"
)

// FIXME(tsileo): take a ctx as first arg for each method

// TODO(tsileo): config as struct
var DefaultConf = map[string]interface{}{
	"backends": map[string]interface{}{
		"blobs": map[string]interface{}{
			"backend-type": "blobsfile",
			"backend-args": map[string]interface{}{
				"path": "$VAR/blobs",
			},
		},
	},
	"router":    []interface{}{[]interface{}{"default", "blobs"}},
	"data_path": pathutil.VarDir(),
}

type BlobStore struct {
	Router *router.Router
}

func New() (*BlobStore, error) {
	conf := DefaultConf
	// Intialize the router and load the backends
	r := router.New(conf["router"].([]interface{}))
	backendsConf := conf["backends"].(map[string]interface{})
	for _, b := range r.ResolveBackends() {
		r.Backends[b] = config.NewFromConfig(backendsConf[b].(map[string]interface{}))
	}
	return &BlobStore{
		Router: r,
	}, nil
}

func (bs *BlobStore) Put(ctx context.Context, blob *blob.Blob) error {
	backend := bs.Router.Route(ctx)

	// Check if the blob already exists
	exists, err := backend.Exists(blob.Hash)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}

	// Save the blob if needed
	// XXX(tsileo): ensure the hash get verified somewhere
	if err := backend.Put(blob.Hash, blob.Data); err != nil {
		return err
	}
	return nil
}

func (bs *BlobStore) Get(ctx context.Context, hash string) ([]byte, error) {
	return bs.Router.Route(ctx).Get(hash)
}

func (bs *BlobStore) Stat(ctx context.Context, hash string) (bool, error) {
	return bs.Router.Route(ctx).Exists(hash)
}

func (bs *BlobStore) Enumerate() error {
	// FIXME(tsileo): handle enumerate
	return nil
}
