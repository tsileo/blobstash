package stash

import (
	"context"
	"fmt"
	"os"
	"testing"

	log "github.com/inconshreveable/log15"

	"a4.io/blobstash/pkg/blob"
	"a4.io/blobstash/pkg/blobstore"
	"a4.io/blobstash/pkg/hashutil"
	"a4.io/blobstash/pkg/hub"
	"a4.io/blobstash/pkg/kvstore"
	"a4.io/blobstash/pkg/meta"
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
	hub := hub.New(logger.New("app", "hub"), true)
	metaHandler, err := meta.New(logger.New("app", "meta"), hub)
	if err != nil {
		panic(err)
	}
	bsRoot, err := blobstore.New(logger.New("app", "blobstore"), true, dir, nil, hub)
	if err != nil {
		panic(err)
	}
	kvsRoot, err := kvstore.New(logger.New("app", "kvstore"), dir, bsRoot, metaHandler)
	if err != nil {
		panic(err)
	}

	s, err := New("stashtest2", metaHandler, bsRoot, kvsRoot, hub, logger)
	if err != nil {
		panic(err)
	}
	defer s.Close()

	blobsRoot, _, err := s.rootDataContext.bs.Enumerate(context.Background(), "", "\xff", 0)
	if err != nil {
		panic(err)
	}
	if len(blobsRoot) != 0 {
		t.Errorf("root blobstore should be empty")
	}

	tmpDataContext, err := s.NewDataContext("tmp")
	if err != nil {
		panic(err)
	}
	blobsIdx := map[string]*blob.Blob{}
	for i := 0; i < 5; i++ {
		b := makeBlob([]byte(fmt.Sprintf("hello%d", i)))
		if _, err := tmpDataContext.bsProxy.Put(context.TODO(), b); err != nil {
			panic(err)
		}
		blobsIdx[b.Hash] = b
	}

	blobsRoot, _, err = s.Root().BlobStore().Enumerate(context.Background(), "", "\xff", 0)
	if err != nil {
		panic(err)
	}
	if len(blobsRoot) != 0 {
		t.Errorf("root blobstore should be empty")
	}

	if err := s.MergeAndDestroy(context.TODO(), "tmp"); err != nil {
		panic(err)
	}

	blobsRoot, _, err = s.rootDataContext.bs.Enumerate(context.Background(), "", "\xff", 0)
	if err != nil {
		panic(err)
	}
	if len(blobsRoot) != 5 {
		t.Errorf("root blobstore should contains 5 blobs, got %d", len(blobsRoot))
	}

	for _, blobRef := range blobsRoot {
		if _, ok := blobsIdx[blobRef.Hash]; !ok {
			t.Errorf("blob %s should be in the root blobstore", blobRef.Hash)
		}
	}

}
