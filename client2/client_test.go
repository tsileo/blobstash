package blobstore

import (
	"testing"
	"bytes"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/tsileo/blobstash/test"
	"github.com/tsileo/blobstash/client"
)

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func TestClient(t *testing.T) {
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
	cl := New("")
	defer cl.Close()
	testCtx := &client.Ctx{Namespace: ""}

	con := cl.ConnWithCtx(testctx)
	defer con.Close()

	// TODO test basic txinit/txcommit workflow
	// TODO add and test the client reqbuffer
}
