package client

import (
	"os"
	"testing"
)

func TestClientDir(t *testing.T) {
	c, err := NewTestClient()
	defer c.Close()
	defer c.RemoveCache()
	check(err)
	tdir := NewRandomTree(t, ".", 1)
	defer os.RemoveAll(tdir)
	meta, wr, err := c.PutDir(tdir)
	check(err)

	rr, err := c.GetDir(meta.Hash, meta.Name+"_restored")
	defer os.RemoveAll(meta.Name + "_restored")
	check(err)
	if !MatchResult(wr, rr) {
		t.Errorf("Directory %+v not restored successfully, wr:%+v/rr:%+v", meta, wr, rr)
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
	tdir := NewRandomTree(t, ".", 3)
	defer os.RemoveAll(tdir)
	meta, wr, err := c.PutDir(tdir)
	check(err)

	rr, err := c.GetDir(meta.Hash, meta.Name+"_restored")
	defer os.RemoveAll(meta.Name + "_restored")
	check(err)
	if !MatchResult(wr, rr) {
		t.Errorf("Directory %+v not restored successfully, wr:%+v/rr:%+v", meta, wr, rr)
	}
}
