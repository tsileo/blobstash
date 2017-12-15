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
	bs       store.BlobStore
	kvs      store.KvStore
	bsProxy  store.BlobStore
	kvsProxy store.KvStore
	hub      *hub.Hub
	meta     *meta.Meta
	log      log.Logger
	dir      string
	root     bool
	closed   bool
}

func (dc *dataContext) BlobStore() store.BlobStore {
	return dc.bs
}

func (dc *dataContext) KvStore() store.KvStore {
	return dc.kvs
}

func (dc *dataContext) BlobStoreProxy() store.BlobStore {
	return dc.bsProxy
}

func (dc *dataContext) KvStoreProxy() store.KvStore {
	return dc.kvsProxy
}

func (dc *dataContext) Closed() bool {
	return dc.closed
}

func (dc *dataContext) Merge(ctx context.Context) error {
	if dc.root {
		return nil
	}

	blobs, _, err := dc.bs.Enumerate(ctx, "", "\xff", 0)
	if err != nil {
		return err
	}
	for _, blobRef := range blobs {
		data, err := dc.bs.Get(ctx, blobRef.Hash)
		if err != nil {
			return err
		}
		b := &blob.Blob{Hash: blobRef.Hash, Data: data}
		if err := dc.bsProxy.(*store.BlobStoreProxy).ReadSrc.Put(ctx, b); err != nil {
			return err
		}
	}

	return dc.Destroy()
}

func (dc *dataContext) Close() error {
	if dc.closed || dc.root {
		return nil
	}
	// TODO(tsileo): multi error
	if err := dc.kvs.Close(); err != nil {
		return err
	}
	if err := dc.bs.Close(); err != nil {
		return err
	}
	dc.closed = true
	return nil
}

func (dc *dataContext) Destroy() error {
	if dc.root {
		return nil
	}
	if err := dc.Close(); err != nil {
		return err
	}
	// TODO(tsileo): only call Destroy from Stash and unexport this one, also remove from index
	return os.RemoveAll(dc.dir)
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
			bs:       bs,
			kvs:      kvs,
			bsProxy:  bs,
			kvsProxy: kvs,
			hub:      h,
			meta:     m,
			log:      l,
			root:     true,
		},
	}

	// TODO(tsileo): list an load the existing stashes
	if err := s.newDataContext("tmp"); err != nil {
		return nil, err
	}

	// FIXME(tsileo): BlobStore.Scan should be triggered here, and for all available stashes

	return s, nil

}

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
	// XXX(tsileo): use a dumb single file cache instead of the blobstore?
	bsDst, err := blobstore.New(l.New("app", "blobstore"), path, nil, h)
	if err != nil {
		return err
	}
	bs := &store.BlobStoreProxy{
		BlobStore: bsDst,
		ReadSrc:   s.rootDataContext.bs,
	}
	kvsDst, err := kvstore.New(l.New("app", "kvstore"), "", bs, m)
	if err != nil {
		return err
	}
	kvs := &store.KvStoreProxy{
		KvStore: kvsDst,
		ReadSrc: s.rootDataContext.kvs,
	}
	dataCtx := &dataContext{
		log:      l,
		meta:     m,
		hub:      h,
		bs:       bsDst,
		kvs:      kvsDst,
		kvsProxy: kvs,
		bsProxy:  bs,
		dir:      path,
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

func (s *Stash) Root() store.DataContext {
	return s.rootDataContext
}

func (s *Stash) dataContext(ctx context.Context) (*dataContext, error) {
	// TODO(tsileo): handle destroyed context
	// FIXME(tsileo): add a BlobStash-Stash-Name header
	return s.rootDataContext, nil
}

func (s *Stash) ContextNames() []string {
	s.Lock()
	defer s.Unlock()
	var out []string
	for k, _ := range s.contexes {
		out = append(out, k)
	}
	return out
}

func (s *Stash) DataContextByName(name string) (*dataContext, bool) {
	if name == "" {
		return s.rootDataContext, true
	}

	if dc, ok := s.contexes[name]; ok {
		return dc, true
	}

	return nil, false
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
	return dataContext.BlobStoreProxy().Put(ctx, blob)
}

func (bs *BlobStore) Get(ctx context.Context, hash string) ([]byte, error) {
	dataContext, err := bs.s.dataContext(ctx)
	if err != nil {
		return nil, err
	}
	return dataContext.BlobStoreProxy().Get(ctx, hash)

}
func (bs *BlobStore) Stat(ctx context.Context, hash string) (bool, error) {
	dataContext, err := bs.s.dataContext(ctx)
	if err != nil {
		return false, err
	}
	return dataContext.BlobStoreProxy().Stat(ctx, hash)

}

func (bs *BlobStore) Enumerate(ctx context.Context, start, end string, limit int) ([]*blob.SizedBlobRef, string, error) {
	dataContext, err := bs.s.dataContext(ctx)
	if err != nil {
		return nil, "", err
	}
	return dataContext.BlobStoreProxy().Enumerate(ctx, start, end, limit)
}
