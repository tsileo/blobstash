package client2

import (
	"testing"

	"github.com/garyburd/redigo/redis"
	"github.com/tsileo/blobstash/test"
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
	cl, err := New("")
	check(err)
	defer cl.Close()
	testCtx := &Ctx{Namespace: ""}

	con := cl.ConnWithCtx(testCtx)
	defer con.Close()

	debug, err := test.GetDebug()
	check(err)
	t.Logf("Debug: %+v", debug)

	// TODO test basic txinit/txcommit workflow
	// TODO add and test the client reqbuffer
}
