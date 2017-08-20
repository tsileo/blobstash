package stash // import "a4.io/blobstash/pkg/stash"

import (
	"context"
	"os"
	"path/filepath"
	"sync"

	log "github.com/inconshreveable/log15"

	"a4.io/blobstash/pkg/blob"
	"a4.io/blobstash/pkg/blobstore"
	_ "a4.io/blobstash/pkg/ctxutil"
	"a4.io/blobstash/pkg/hub"
	"a4.io/blobstash/pkg/kvstore"
	"a4.io/blobstash/pkg/meta"
	"a4.io/blobstash/pkg/stash/store"
)

type dataContext struct {
	bs   store.BlobStore
	kvs  store.KvStore
	hub  *hub.Hub
	meta *meta.Meta
	log  log.Logger
}

func (dc *dataContext) Close() error {
	// TODO(tsileo): multi error
	if err := dc.kvs.Close(); err != nil {
		return err
	}
	if err := dc.bs.Close(); err != nil {
		return err
	}
	return nil
}

type Stash struct {
	rootDataContext *dataContext
	contexes        map[string]*dataContext
	path            string
	sync.Mutex
}

func New(dir string, m *meta.Meta, bs *blobstore.BlobStore, kvs *kvstore.KvStore, h *hub.Hub, l log.Logger) (*Stash, error) {
	s := &Stash{
		contexes: map[string]*dataContext{},
		path:     dir,
		rootDataContext: &dataContext{
			bs:   bs,
			kvs:  kvs,
			hub:  h,
			meta: m,
			log:  l,
		},
	}

	// TODO(tsileo): list an load the existing stashes
	if err := s.newDataContext("tmp"); err != nil {
		return nil, err
	}

	// FIXME(tsileo): BlobStore.Scan should be triggered here, and for all available stashes

	return s, nil

}

// TODO(tsileo): implements a blobstore/kvstore that statisfy the store.BlobStore/KvStore interface and
// proxy all read operations to the rootDataProxy if it failed

func (s Stash) newDataContext(name string) error {
	s.Lock()
	defer s.Unlock()
	path := filepath.Join(s.path, name)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		if err := os.MkdirAll(path, 0700); err != nil {
			return err
		}
	}
	l := s.rootDataContext.log.New("data_ctx", name)
	h := hub.New(l.New("app", "hub"))
	m, err := meta.New(l.New("app", "meta"), h)
	if err != nil {
		return err
	}
	bsDst, err := blobstore.New(l.New("app", "blobstore"), s.path, nil, h)
	if err != nil {
		return err
	}
	bs := &store.BlobStoreProxy{
		BlobStore: bsDst,
		ReadSrc:   s.rootDataContext.bs,
	}
	kvsDst, err := kvstore.New(l.New("app", "kvstore"), s.path, bs, m)
	if err != nil {
		return err
	}
	kvs := &store.KvStoreProxy{
		KvStore: kvsDst,
		ReadSrc: s.rootDataContext.kvs,
	}
	dataCtx := &dataContext{
		log:  l,
		meta: m,
		hub:  h,
		kvs:  kvs,
		bs:   bs,
	}
	s.contexes[name] = dataCtx
	return nil
}

func (s *Stash) Close() error {
	s.rootDataContext.Close()
	s.Lock()
	defer s.Unlock()
	for _, dc := range s.contexes {
		dc.Close()
	}
	return nil
}

func (s *Stash) dataContext(ctx context.Context) (*dataContext, error) {
	return s.rootDataContext, nil
}

func (s *Stash) BlobStore() *BlobStore {
	return &BlobStore{s}
}

type BlobStore struct {
	s *Stash
}

func (bs *BlobStore) Close() error { return nil }

func (bs *BlobStore) Put(ctx context.Context, blob *blob.Blob) error {
	dataContext, err := bs.s.dataContext(ctx)
	if err != nil {
		return err
	}
	return dataContext.bs.Put(ctx, blob)
}

func (bs *BlobStore) Get(ctx context.Context, hash string) ([]byte, error) {
	dataContext, err := bs.s.dataContext(ctx)
	if err != nil {
		return nil, err
	}
	return dataContext.bs.Get(ctx, hash)

}
func (bs *BlobStore) Stat(ctx context.Context, hash string) (bool, error) {
	dataContext, err := bs.s.dataContext(ctx)
	if err != nil {
		return false, err
	}
	return dataContext.bs.Stat(ctx, hash)

}

func (bs *BlobStore) Enumerate(ctx context.Context, start, end string, limit int) ([]*blob.SizedBlobRef, string, error) {
	dataContext, err := bs.s.dataContext(ctx)
	if err != nil {
		return nil, "", err
	}
	return dataContext.bs.Enumerate(ctx, start, end, limit)
}

// 	if ns, ok := ctxutil.Namespace(ctx); ok {
