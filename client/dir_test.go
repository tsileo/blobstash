package client

import (
	"os"
	"testing"

	"github.com/tsileo/blobstash/test"
)

func TestClientDir(t *testing.T) {
	s, err := test.NewTestServer()
	check(err)
	go s.Start()
	if err := s.TillReady(); err != nil {
		t.Fatalf("server error:\n%v", err)
	}
	defer s.Shutdown()
	c, err := NewTestClient()
	defer c.Close()
	defer c.RemoveCache()
	check(err)
	tdir := test.NewRandomTree(t, ".", 1)
	defer os.RemoveAll(tdir)
	meta, wr, err := c.PutDir(tdir)
	check(err)

	// Move this to test client
	//hostname, err := os.Hostname()
	//check(err)

	//hosts, err := c.Hosts()
	//check(err)
	//if len(hosts) != 1 || hosts[0] != hostname {
	//	t.Errorf("Hosts() should return [%v], got %q", hostname, hosts)
	//}

	rr, err := c.GetDir(meta.Hash, meta.Name+"_restored")
	defer os.RemoveAll(meta.Name+"_restored")
	check(err)
	if !MatchResult(wr, rr) {
		t.Errorf("Directory %+v not restored successfully, wr:%+v/rr:%+v", meta, wr, rr)
	}
	check(test.Diff(tdir, meta.Name+"_restored"))
}

func TestClientDirDeepRecursion(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping TestClientDirDeepRecursion test in short mode.")
	}
	s, err := test.NewTestServer()
	check(err)
	go s.Start()
	s.TillReady()
	defer s.Shutdown()

	c, err := NewTestClient()
	check(err)
	defer c.Close()
	defer c.RemoveCache()
	tdir := test.NewRandomTree(t, ".", 5)
	defer os.RemoveAll(tdir)
	meta, wr, err := c.PutDir(tdir)
	check(err)

	rr, err := c.GetDir(meta.Hash, meta.Name+"_restored")
	defer os.RemoveAll(meta.Name + "_restored")
	check(err)
	if !MatchResult(wr, rr) {
		t.Errorf("Directory %+v not restored successfully, wr:%+v/rr:%+v", meta, wr, rr)
	}
	check(test.Diff(tdir, meta.Name+"_restored"))
}
