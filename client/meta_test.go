package client

import (
	"github.com/garyburd/redigo/redis"
	"testing"

	"github.com/tsileo/blobstash/test"
)

func TestModelsMeta(t *testing.T) {
	s, err := test.NewTestServer(t)
	check(err)
	go s.Start()
	if err := s.TillReady(); err != nil {
		t.Fatalf("server error:\n%v", err)
	}
	defer s.Shutdown()
	c, err := NewTestClient()
	check(err)
	defer c.Close()
	con := c.Pool.Get()
	defer con.Close()
	txId, err := redis.String(con.Do("TXINIT"))
	check(err)
	f := NewMeta()
	f.Name = "ok"
	f.Ref = "ok"
	err = f.Save(txId, c.Pool)
	check(err)
	_, err = con.Do("TXCOMMIT")
	check(err)

	fe, err := NewMetaFromDB(c.Pool, f.Hash)
	check(err)
	if f.Hash != fe.Hash {
		t.Errorf("Error retrieving Meta from DB, expected %+v, get %+v", f, fe)
	}
	//NewRandomTree(t, ".", 3)
}
