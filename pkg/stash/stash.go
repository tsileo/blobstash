package stash // import "a4.io/blobstash/pkg/stash"

import (
	_ "context"
	"os"
	"path/filepath"
	"sync"

	log "github.com/inconshreveable/log15"

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

	if err := s.newDataContext("tmp"); err != nil {
		return nil, err
	}

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
	bs, err := blobstore.New(l.New("app", "blobstore"), s.path, nil, h)
	if err != nil {
		return err
	}
	kvs, err := kvstore.New(l.New("app", "kvstore"), s.path, bs, m)
	if err != nil {
		return err
	}

	dataCtx := &dataContext{
		log:  l,
		meta: m,
		hub:  h,
		kvs:  kvs,
		bs: &store.BlobStoreProxy{
			BlobStore: bs,
			ReadSrc:   s.rootDataContext.bs,
		},
	}
	s.contexes[name] = dataCtx
	return nil
}

func (s *Stash) Close() error {
	// TODO(tsileo): clean shutdown
	return nil
}

// func (s *Stash) BlobStore(ctx context.Context) (*blobstore.BlobStore, bool) {
// 	if ns, ok := ctxutil.Namespace(ctx); ok {
// 		return nil, false
// 	}
// 	return s.bs, true
// }

// func (s *Stash) KvStore(ctx context.Context) (*kvstore.KvStore, bool) {
// 	return s.kvs, true
// }

// func (s *Stash) Hub(ctx context.Context) (*hub.Hub, bool) {
// 	return s.hub, true
// }

// func (s *Stash) Meta(ctx context.Context) (*meta.Meta, bool) {
// 	return s.meta, true
// }
