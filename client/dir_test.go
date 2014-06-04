package client

import (
	"os"
	"testing"

	"github.com/garyburd/redigo/redis"
)

func TestClientDir(t *testing.T) {
	c, err := NewTestClient()
	defer c.Close()
	defer c.RemoveCache()
	check(err)
	con := c.Pool.Get()
	defer con.Close()
	txID, err := redis.String(con.Do("TXINIT"))
	check(err)
	tdir := NewRandomTree(t, ".", 1)
	defer os.RemoveAll(tdir)
	meta, wr, err := c.PutDir(txID, tdir)
	check(err)

	_, err = con.Do("TXCOMMIT")
	check(err)

	rr, err := c.GetDir(meta.Hash, meta.Name+"_restored")
	defer os.RemoveAll(meta.Name + "_restored")
	check(err)
	if !MatchResult(wr, rr) {
		t.Error("Directory not restored successfully, wr:%+v/rr:%+v", wr, rr)
	}
}

func TestClientDirDeepRecursion(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping TestClientDirDeepRecursion test in short mode.")
	}
	c, err := NewTestClient()
	check(err)
	defer c.Close()
	defer c.RemoveCache()
	con := c.Pool.Get()
	defer con.Close()
	txID, err := redis.String(con.Do("TXINIT"))
	check(err)
	tdir := NewRandomTree(t, ".", 3)
	defer os.RemoveAll(tdir)
	meta, wr, err := c.PutDir(txID, tdir)
	check(err)

	_, err = con.Do("TXCOMMIT")
	check(err)

	rr, err := c.GetDir(meta.Hash, meta.Name+"_restored")
	defer os.RemoveAll(meta.Name + "_restored")
	check(err)
	if !MatchResult(wr, rr) {
		t.Error("Directory not restored successfully, wr:%+v/rr:%+v", wr, rr)
	}
}
