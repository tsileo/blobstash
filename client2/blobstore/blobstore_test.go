package blobstore

import (
	"testing"

	"github.com/garyburd/redigo/redis"
	"github.com/tsileo/blobstash/test"
	"github.com/tsileo/blobstash/client"
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
	exist, err := bs.Stat(&client.Ctx{Namespace: ""}, "f9c24e2abb82063a3ba2c44efd2d3c797f28ac90")
	check(err)
	if exist {
		t.Errorf("Blob f9c24e2abb82063a3ba2c44efd2d3c797f28ac90 should not exists")
	}
}
