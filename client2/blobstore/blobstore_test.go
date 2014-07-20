package blobstore

import (
	"testing"
	"bytes"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/tsileo/blobstash/test"
	"github.com/tsileo/blobstash/client2/ctx"
)

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func TestBlobStore(t *testing.T) {
	s, err := test.NewTestServer(t)
	check(err)
	go s.Start()
	if err := s.TillReady(); err != nil {
		t.Fatalf("server error:\n%v", err)
	}
	defer s.Shutdown()
	c, err := redis.Dial("tcp", ":9735")
	check(err)
	defer c.Close()
	if _, err := c.Do("PING"); err != nil {
		t.Errorf("PING failed")
	}
	bs := New("")
	testCtx := &ctx.Ctx{Namespace: ""}
	exist, err := bs.Stat(testCtx, "f9c24e2abb82063a3ba2c44efd2d3c797f28ac90")
	check(err)
	if exist {
		t.Errorf("Blob f9c24e2abb82063a3ba2c44efd2d3c797f28ac90 should not exists")
	}

	blob := test.RandomBlob(nil)
	err = bs.Put(testCtx, blob.Hash, blob.Data)
	check(err)

	// Wait a little since putting blob is an async operation
	time.Sleep(500*time.Millisecond)

	blobData, err := bs.Get(testCtx, blob.Hash)
	if !bytes.Equal(blob.Data, blobData) {
		t.Errorf("Blob corrupted, expected: %v, got %v", blob.Data, blobData)
	}
	exist, err = bs.Stat(testCtx, blob.Hash)
	check(err)
	if !exist {
		t.Errorf("Blob %v should not exists", blob.Hash)
	}
}
