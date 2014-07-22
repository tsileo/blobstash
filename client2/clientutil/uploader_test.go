package clientutil

import (
	"testing"
	"time"

	"github.com/garyburd/redigo/redis"

	"github.com/tsileo/blobstash/test"
	client "github.com/tsileo/blobstash/client2"
	"github.com/tsileo/blobstash/client2/ctx"
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

	meta, wr, err := up.PutFile(testCtx, nil, "/work/writing/cube.md")
	check(err)

	time.Sleep(1*time.Second)

	rr, err := GetFile(cl, testCtx, meta.Hash, "cube2.md")
	check(err)
	t.Logf("%v %v %v %v %v", up, testCtx, meta, wr, rr)
}
