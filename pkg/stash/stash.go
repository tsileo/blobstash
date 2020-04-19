package stash // import "a4.io/blobstash/pkg/stash"

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"

	log "github.com/inconshreveable/log15"

	"a4.io/blobsfile"
	"a4.io/blobstash/pkg/blob"
	"a4.io/blobstash/pkg/blobstore"
	"a4.io/blobstash/pkg/ctxutil"
	"a4.io/blobstash/pkg/filetree/filetreeutil/node"
	"a4.io/blobstash/pkg/hub"
	"a4.io/blobstash/pkg/kvstore"
	"a4.io/blobstash/pkg/meta"
	"a4.io/blobstash/pkg/stash/store"
	"a4.io/blobstash/pkg/vkv"
)

type dataContext struct {
	bs       store.BlobStore
	kvs      store.KvStore
	bsDst    store.BlobStore
	bsProxy  store.BlobStore
	kvsProxy store.KvStore
	hub      *hub.Hub
	meta     *meta.Meta
	log      log.Logger
	dir      string
	root     bool
	closed   bool
}

func (dc *dataContext) StashBlobStore() store.BlobStore {
	return dc.bsDst
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
		if _, err := dc.bsProxy.(*store.BlobStoreProxy).ReadSrc.Put(ctx, b); err != nil {
			return err
		}
	}

	return nil
}

// orderedRefs holds the "sorted" references
type orderedRefs struct {
	refs []string
	idx  map[string]struct{}
}

func newOrderedRefs() *orderedRefs {
	return &orderedRefs{
		refs: []string{},
		idx:  map[string]struct{}{},
	}
}

func (r *orderedRefs) Add(ref string) {
	if _, ok := r.idx[ref]; !ok {
		r.idx[ref] = struct{}{}
		r.refs = append(r.refs, ref)
	}
}

func (dc *dataContext) MergeFileTreeNode(ctx context.Context, refs *orderedRefs, bs store.BlobStore, ref string) error {
	data, err := dc.bsProxy.Get(ctx, ref)
	if err != nil {
		return err
	}

	n, err := node.NewNodeFromBlob(ref, data)
	if err != nil {
		return err
	}

	if n.Type == "file" {
		for _, dref := range n.Refs {
			// Save each blob content
			data := dref.([]interface{})
			bref := data[1].(string)
			refs.Add(bref)
		}
	} else {
		// Iter the dir
		for _, cref := range n.Refs {
			// Merge the children recursively
			if err := dc.MergeFileTreeNode(ctx, refs, bs, cref.(string)); err != nil {
				return err
			}
		}
	}

	// Only save the node ref once all it's children has been saved
	refs.Add(ref)

	return nil
}

func (dc *dataContext) MergeFileTreeVersion(ctx context.Context, key string, version int64) (*orderedRefs, error) {
	if dc.root {
		return nil, fmt.Errorf("cannot merge filtree version in root data context")
	}

	refs := newOrderedRefs()

	// Fetch the root BlobStore (as fetching the original struct behind the interface is costly)
	rootBs := dc.bsProxy.(*store.BlobStoreProxy).ReadSrc

	// Fetch the blob that contains the KV entry for the FileTree version
	kvBlobRef, err := dc.kvs.GetMetaBlob(ctx, key, version)
	if err != nil {
		return nil, err
	}

	// Now, traverse the tree, starting at the root
	kv, err := dc.kvs.Get(ctx, key, version)
	if err != nil {
		return nil, err
	}

	// Merge the root node recursively
	ftRoot := kv.HexHash()
	if err := dc.MergeFileTreeNode(ctx, refs, rootBs, ftRoot); err != nil {
		return nil, err
	}

	refs.Add(kvBlobRef)

	return refs, nil
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

func (s *Stash) destroy(dataContext *dataContext, name string) error {
	if dataContext.root {
		return fmt.Errorf("cannot destroy the root data context")
	}

	delete(s.contexes, name)

	if err := dataContext.Destroy(); err != nil {
		return err
	}

	return nil
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

			root: true,
		},
	}

	stashes, err := ioutil.ReadDir(dir)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	if err == nil {
		for _, dir := range stashes {
			if _, err := s.NewDataContext(dir.Name()); err != nil {
				return nil, err
			}
		}
	}

	// FIXME(tsileo): BlobStore.Scan should be triggered here??, and for all available stashes

	return s, nil

}

func (s *Stash) NewDataContext(name string) (*dataContext, error) {
	s.Lock()
	defer s.Unlock()
	path := filepath.Join(s.path, name)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		if err := os.MkdirAll(path, 0700); err != nil {
			return nil, err
		}
	}
	l := s.rootDataContext.log.New("data_ctx", name)
	h := hub.New(l.New("app", "hub"), false)
	m, err := meta.New(l.New("app", "meta"), h)
	if err != nil {
		return nil, err
	}
	// XXX(tsileo): use a dumb single file cache instead of the blobstore?
	bsDst, err := blobstore.New(l.New("app", "blobstore"), false, path, nil, h)
	if err != nil {
		return nil, err
	}
	bs := &store.BlobStoreProxy{
		BlobStore: bsDst,
		ReadSrc:   s.rootDataContext.bs,
	}
	kvsDst, err := kvstore.New(l.New("app", "kvstore"), path, bs, m)
	if err != nil {
		return nil, err
	}
	kvs := &store.KvStoreProxy{
		KvStore: kvsDst,
		ReadSrc: s.rootDataContext.kvs,
	}
	dataCtx := &dataContext{
		bsDst:    bsDst,
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
	return dataCtx, nil
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

func (s *Stash) DoAndDestroy(ctx context.Context, name string, do func(context.Context, store.DataContext) error) error {
	s.Lock()
	dc, ok := s.contexes[name]
	if !ok {
		s.Unlock()
		return fmt.Errorf("data context not found")
	}
	s.Unlock()

	if err := do(ctx, dc); err != nil {
		return err
	}

	s.Lock()
	defer s.Unlock()
	if err := s.destroy(dc, name); err != nil {
		return err
	}

	return nil
}

func (s *Stash) MergeFileTreeVersionAndDestroy(ctx context.Context, name string, key string, version int64) error {
	s.Lock()
	defer s.Unlock()
	dc, ok := s.contexes[name]
	if !ok {
		return fmt.Errorf("data context not found")
	}

	refs, err := dc.MergeFileTreeVersion(ctx, key, version)
	if err != nil {
		return err
	}

	var blobsCnt int
	var totalSize uint64
	for _, ref := range refs.refs {
		// Get the marked blob from the blobstore proxy
		data, err := dc.StashBlobStore().Get(ctx, ref)
		if err != nil {
			if err == blobsfile.ErrBlobNotFound {
				continue
			}
			return err
		}

		// Save it in the root blobstore
		saved, err := s.Root().BlobStore().Put(ctx, &blob.Blob{Hash: ref, Data: data})
		if err != nil {
			return err
		}

		if saved {
			blobsCnt++
			totalSize += uint64(len(data))
		}
	}
	fmt.Printf("GC/merge filetree refs=%d blobs, saved %d blobs\n", len(refs.refs), blobsCnt)

	if err := s.destroy(dc, name); err != nil {
		return err
	}

	return nil
}

func (s *Stash) MergeAndDestroy(ctx context.Context, name string) error {
	s.Lock()
	defer s.Unlock()
	dc, ok := s.contexes[name]
	if !ok {
		return fmt.Errorf("data context not found")
	}

	if err := dc.Merge(ctx); err != nil {
		return err
	}

	if err := s.destroy(dc, name); err != nil {
		return err
	}

	return nil
}

func (s *Stash) Destroy(ctx context.Context, name string) error {
	s.Lock()
	defer s.Unlock()
	dc, ok := s.contexes[name]
	if !ok {
		return fmt.Errorf("data context not found")
	}

	if err := s.destroy(dc, name); err != nil {
		return err
	}

	return nil
}

func (s *Stash) dataContext(ctx context.Context) (*dataContext, error) {
	// TODO(tsileo): handle destroyed context
	name, _ := ctxutil.Namespace(ctx)
	if ctx, ok := s.DataContextByName(name); ok {
		return ctx, nil
	}

	// If it does not exist, create it now
	return s.NewDataContext(name)
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

	// FIXME(tsileo): fix the deadlock
	s.Lock()
	defer s.Unlock()
	if dc, ok := s.contexes[name]; ok {
		return dc, true
	}

	return nil, false
}

func (s *Stash) BlobStore() *BlobStore {
	return &BlobStore{s}
}

func (s *Stash) KvStore() *KvStore {
	return &KvStore{s}
}

type BlobStore struct {
	s *Stash
}

func (bs *BlobStore) Close() error { return nil } // TODO(tsileo): check if no closing is needed?

func (bs *BlobStore) Put(ctx context.Context, blob *blob.Blob) (bool, error) {
	dataContext, err := bs.s.dataContext(ctx)
	if err != nil {
		return false, err
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

type KvStore struct {
	s *Stash
}

func (kv *KvStore) Close() error { return nil }

func (kv *KvStore) Put(ctx context.Context, key, ref string, data []byte, version int64) (*vkv.KeyValue, error) {
	dataContext, err := kv.s.dataContext(ctx)
	if err != nil {
		return nil, err
	}
	return dataContext.KvStoreProxy().Put(ctx, key, ref, data, version)
}

func (kv *KvStore) Get(ctx context.Context, key string, version int64) (*vkv.KeyValue, error) {
	dataContext, err := kv.s.dataContext(ctx)
	if err != nil {
		return nil, err
	}
	return dataContext.KvStoreProxy().Get(ctx, key, version)
}

func (kv *KvStore) GetMetaBlob(ctx context.Context, key string, version int64) (string, error) {
	dataContext, err := kv.s.dataContext(ctx)
	if err != nil {
		return "", err
	}
	return dataContext.KvStoreProxy().GetMetaBlob(ctx, key, version)
}

func (kv *KvStore) Versions(ctx context.Context, key, start string, limit int) (*vkv.KeyValueVersions, string, error) {
	dataContext, err := kv.s.dataContext(ctx)
	if err != nil {
		return nil, "", err
	}
	return dataContext.KvStoreProxy().Versions(ctx, key, start, limit)
}

func (kv *KvStore) Keys(ctx context.Context, start, end string, limit int) ([]*vkv.KeyValue, string, error) {
	dataContext, err := kv.s.dataContext(ctx)
	if err != nil {
		return nil, "", err
	}
	return dataContext.KvStoreProxy().Keys(ctx, start, end, limit)
}

func (kv *KvStore) ReverseKeys(ctx context.Context, start, end string, limit int) ([]*vkv.KeyValue, string, error) {
	dataContext, err := kv.s.dataContext(ctx)
	if err != nil {
		return nil, "", err
	}
	return dataContext.KvStoreProxy().ReverseKeys(ctx, start, end, limit)
}
