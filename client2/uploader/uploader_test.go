package uploader

import (
	"testing"

	"github.com/garyburd/redigo/redis"

	"github.com/tsileo/blobstash/test"
	"github.com/tsileo/blobstash/client2"
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

	cl, err := client2.New("")
	check(err)
	up := &Uploader{
		client: cl,
	}
	testCtx := &ctx.Ctx{Namespace: ""}

	meta, err := up.PutFile(testCtx, nil, "/work/writing/cube.md")
	check(err)

	t.Logf("%v %v %v", up, testCtx, meta)
}
