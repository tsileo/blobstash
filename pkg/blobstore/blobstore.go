package blobstore

import (
	log "github.com/inconshreveable/log15"
	"golang.org/x/net/context"

	_ "github.com/tsileo/blobstash/backend"
	_ "github.com/tsileo/blobstash/config"
	"github.com/tsileo/blobstash/config/pathutil"
	"github.com/tsileo/blobstash/pkg/backend/blobsfile"
	"github.com/tsileo/blobstash/pkg/blob"
	"github.com/tsileo/blobstash/pkg/ctxutil"
	"github.com/tsileo/blobstash/pkg/hub"
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
	back   *blobsfile.BlobsFileBackend
	hub    *hub.Hub

	log log.Logger
}

func New(logger log.Logger, hub *hub.Hub) (*BlobStore, error) {
	logger.Debug("init")
	conf := DefaultConf
	// Intialize the router and load the backends
	r := router.New(conf["router"].([]interface{}))
	backendsConf := conf["backends"].(map[string]interface{})
	for _, b := range r.ResolveBackends() {
		r.Backends[b] = blobsfile.NewFromConfig(backendsConf[b].(map[string]interface{}))
	}
	return &BlobStore{
		Router: r,
		hub:    hub,
		log:    logger,
	}, nil
}

func (bs *BlobStore) Close() error {
	// TODO(tsileo): improve this
	for _, back := range bs.Router.Backends {
		back.Close()
	}
	return nil
}

func (bs *BlobStore) Put(ctx context.Context, blob *blob.Blob) error {
	_, fromHttp := ctxutil.Request(ctx)
	bs.log.Info("OP Put", "from_http", fromHttp, "hash", blob.Hash, "len", len(blob.Data))
	backend := bs.Router.Route(ctx)
	// Check if the blob already exists
	exists, err := backend.Exists(blob.Hash)
	if err != nil {
		return err
	}
	if exists {
		bs.log.Debug("blob already exists", "hash", blob.Hash)
		return nil
	}
	// Recompute the blob to ensure it's not corrupted
	if err := blob.Check(); err != nil {
		return err
	}
	// Save the blob if needed
	if err := backend.Put(blob.Hash, blob.Data); err != nil {
		return err
	}
	// TODO(tsileo): make this async with the put blob
	if err := bs.hub.NewBlobEvent(ctx, blob); err != nil {
		return err
	}
	bs.log.Debug("blob saved", "hash", blob.Hash)
	return nil
}

func (bs *BlobStore) Get(ctx context.Context, hash string) ([]byte, error) {
	_, fromHttp := ctxutil.Request(ctx)
	bs.log.Info("OP Get", "from_http", fromHttp, "hash", hash)
	return bs.Router.Route(ctx).Get(hash)
}

func (bs *BlobStore) Stat(ctx context.Context, hash string) (bool, error) {
	_, fromHttp := ctxutil.Request(ctx)
	bs.log.Info("OP Stat", "from_http", fromHttp, "hash", hash)
	return bs.Router.Route(ctx).Exists(hash)
}

// func (backend *BlobsFileBackend) Enumerate(blobs chan<- *blob.SizedBlobRef, start, stop string, limit int) error {

func (bs *BlobStore) Enumerate(ctx context.Context, start, end string, limit int) ([]*blob.SizedBlobRef, error) {
	_, fromHttp := ctxutil.Request(ctx)
	bs.log.Info("OP Enumerate", "from_http", fromHttp, "start", start, "end", end, "limit", limit)
	out := make(chan *blob.SizedBlobRef)
	refs := []*blob.SizedBlobRef{}
	errc := make(chan error, 1)
	back := bs.Router.Route(ctx).(*blobsfile.BlobsFileBackend)
	// TODO(tsileo): implements `Enumerate2`
	go func() {
		errc <- back.Enumerate2(out, start, end, limit)
	}()
	for blob := range out {
		refs = append(refs, blob)
	}
	if err := <-errc; err != nil {
		return nil, err
	}
	// FIXME(tsileo): handle enumerate
	return refs, nil
}
