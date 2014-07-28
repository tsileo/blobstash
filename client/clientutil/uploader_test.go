package clientutil

import (
	"testing"
	"time"
	"os"

	"github.com/garyburd/redigo/redis"

	"github.com/tsileo/blobstash/client"
	"github.com/tsileo/blobstash/client/ctx"
	"github.com/tsileo/blobstash/test"
)

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func TestUploader(t *testing.T) {
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

	cl, err := client.New("")
	check(err)
	up := NewUploader(cl)
	testCtx := &ctx.Ctx{Namespace: ""}

	t.Logf("Testing with a random file...")

	fname := test.NewRandomFile(".")
	defer os.Remove(fname)
	meta, wr, err := up.PutFile(testCtx, nil, fname)
	check(err)

	time.Sleep(1 * time.Second)

	rr, err := GetFile(cl, testCtx, meta.Hash, fname+"restored")
	defer os.Remove(fname+"restored")
	check(err)
	t.Logf("%v %v %v %v %v", up, testCtx, meta, wr, rr)

	t.Logf("Testing with a random directory tree")
	path, _ := test.CreateRandomTree(t, ".", 0, 1)
	defer os.RemoveAll(path)
	meta, wr, err = up.PutDir(testCtx, path)
	check(err)
	t.Logf("%v %v %v %v %v", up, testCtx, meta, wr, rr)

	time.Sleep(3 * time.Second)
	rr, err = GetDir(cl, testCtx, meta.Hash, path+"restored")
	defer os.RemoveAll(path+"restored")
	check(err)
}
