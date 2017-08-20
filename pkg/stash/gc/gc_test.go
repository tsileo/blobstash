package gc

import (
	"context"
	"fmt"
	"os"
	"testing"

	log "github.com/inconshreveable/log15"

	"a4.io/blobstash/pkg/blob"
	bstore "a4.io/blobstash/pkg/blobstore"
	"a4.io/blobstash/pkg/hashutil"
	"a4.io/blobstash/pkg/hub"
	kstore "a4.io/blobstash/pkg/kvstore"
	"a4.io/blobstash/pkg/meta"
	"a4.io/blobstash/pkg/stash"
)

func makeBlob(data []byte) *blob.Blob {
	return &blob.Blob{
		Hash: hashutil.Compute(data),
		Data: data,
	}
}

func TestDataContextMerge(t *testing.T) {
	dir := "stashtest"
	if err := os.MkdirAll(dir, 0700); err != nil {
		panic(err)
	}
	dir2 := "stashtest2"
	if err := os.MkdirAll(dir2, 0700); err != nil {
		panic(err)
	}
	defer func() {
		os.RemoveAll(dir)
		os.RemoveAll(dir2)
	}()
	logger := log.New()
	hub := hub.New(logger.New("app", "hub"))
	metaHandler, err := meta.New(logger.New("app", "meta"), hub)
	if err != nil {
		panic(err)
	}
	bsRoot, err := bstore.New(logger.New("app", "blobstore"), dir, nil, hub)
	if err != nil {
		panic(err)
	}
	kvsRoot, err := kstore.New(logger.New("app", "kvstore"), dir, bsRoot, metaHandler)
	if err != nil {
		panic(err)
	}

	s, err := stash.New("stashtest2", metaHandler, bsRoot, kvsRoot, hub, logger)
	if err != nil {
		panic(err)
	}
	defer s.Close()

	blobsRoot, _, err := s.Root().BlobStore().Enumerate(context.Background(), "", "\xff", 0)
	if err != nil {
		panic(err)
	}
	if len(blobsRoot) != 0 {
		t.Errorf("root blobstore should be empty")
	}

	tmpDataContext, ok := s.DataContextByName("tmp")
	if !ok {
		t.Errorf("tmp data context should exists")
	}
	blobsIdx := map[string]*blob.Blob{}
	var lastBlob *blob.Blob
	for i := 0; i < 5; i++ {
		b := makeBlob([]byte(fmt.Sprintf("hello%d", i)))
		if err := tmpDataContext.BlobStoreProxy().Put(context.TODO(), b); err != nil {
			panic(err)
		}
		blobsIdx[b.Hash] = b
		lastBlob = b
	}

	if _, err := tmpDataContext.KvStore().Put(context.TODO(), "hello", lastBlob.Hash, nil, 10); err != nil {
		panic(err)
	}

	blobsRoot, _, err = s.Root().BlobStore().Enumerate(context.Background(), "", "\xff", 0)
	if err != nil {
		panic(err)
	}
	if len(blobsRoot) != 0 {
		t.Errorf("root blobstore should be empty")
	}

	gc := New(s, tmpDataContext)
	if err := gc.GC(context.Background(), "mark_kv('hello', 10)"); err != nil {
		panic(err)
	}

	blobsRoot, _, err = s.Root().BlobStore().Enumerate(context.Background(), "", "\xff", 0)
	if err != nil {
		panic(err)
	}
	if len(blobsRoot) != 2 {
		t.Errorf("root blobstore should contains 2 blobs, got %d", len(blobsRoot))
	}

	// FIXME(tsileo): try to read the kv and the blob in the root data context

	// if blobsRoot[0].Hash != lastBlob.Hash {
	// t.Errorf("bad GCed blob, expected %s, got %s", lastBlob.Hash, blobsRoot[0].Hash)
	// }
}
