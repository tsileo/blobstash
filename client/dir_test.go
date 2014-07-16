package client

import (
	"os"
	"testing"
	"time"

	"github.com/tsileo/blobstash/test"
)

func TestClientDir(t *testing.T) {
	s, err := test.NewTestServer(t)
	check(err)
	go s.Start()
	if err := s.TillReady(); err != nil {
		t.Fatalf("server error:\n%v", err)
	}
	defer s.Shutdown()
	c, err := NewClient("", ":9735", []string{})
	defer c.Close()
	defer c.RemoveCache()
	check(err)
	tdir := test.NewRandomTree(t, ".", 1)
	defer os.RemoveAll(tdir)
	meta, wr, err := c.PutDir(&Ctx{Namespace: c.Hostname}, tdir)
	check(err)

	// Move this to test client
	//hostname, err := os.Hostname()
	//check(err)

	//hosts, err := c.Hosts()
	//check(err)
	//if len(hosts) != 1 || hosts[0] != hostname {
	//	t.Errorf("Hosts() should return [%v], got %q", hostname, hosts)
	//}

	time.Sleep(2*time.Second)

	rr, err := c.GetDir(&Ctx{Namespace: c.Hostname}, meta.Hash, meta.Name+"_restored")
	defer os.RemoveAll(meta.Name+"_restored")
	check(err)
	if !MatchResult(wr, rr) {
		t.Errorf("Directory %+v not restored successfully, wr:%+v/rr:%+v", meta, wr, rr)
	}
	check(test.Diff(tdir, meta.Name+"_restored"))
}

func TestClientArchiveDir(t *testing.T) {
	s, err := test.NewTestServer(t)
	check(err)
	go s.Start()
	if err := s.TillReady(); err != nil {
		t.Fatalf("server error:\n%v", err)
	}
	defer s.Shutdown()
	c, err := NewClient("", ":9735", []string{})
	defer c.Close()
	defer c.RemoveCache()
	check(err)
	tdir := test.NewRandomTree(t, ".", 1)
	defer os.RemoveAll(tdir)
	meta, wr, err := c.PutDir(&Ctx{Namespace: c.Hostname, Archive: true}, tdir)
	check(err)

	// Move this to test client
	//hostname, err := os.Hostname()
	//check(err)

	//hosts, err := c.Hosts()
	//check(err)
	//if len(hosts) != 1 || hosts[0] != hostname {
	//	t.Errorf("Hosts() should return [%v], got %q", hostname, hosts)
	//}

	time.Sleep(2*time.Second)

	rr, err := c.GetDir(&Ctx{Namespace: c.Hostname, Archive: true}, meta.Hash, meta.Name+"_restored")
	defer os.RemoveAll(meta.Name+"_restored")
	check(err)
	if !MatchResult(wr, rr) {
		t.Errorf("Archive %+v not restored successfully, wr:%+v/rr:%+v", meta, wr, rr)
	}
	check(test.Diff(tdir, meta.Name+"_restored"))
}

func TestClientDirDeepRecursion(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping TestClientDirDeepRecursion test in short mode.")
	}
	s, err := test.NewTestServer(t)
	check(err)
	go s.Start()
	if err := s.TillReady(); err != nil {
		t.Fatalf("server error:\n%v", err)
	}
	defer s.Shutdown()

	c, err := NewClient("", ":9735", []string{})
	check(err)
	defer c.Close()
	defer c.RemoveCache()
	tdir := test.NewRandomTree(t, ".", 5)
	defer os.RemoveAll(tdir)
	meta, wr, err := c.PutDir(&Ctx{Namespace: c.Hostname}, tdir)
	check(err)

	time.Sleep(3*time.Second)

	rr, err := c.GetDir(&Ctx{Namespace: c.Hostname}, meta.Hash, meta.Name+"_restored")
	defer os.RemoveAll(meta.Name + "_restored")
	check(err)
	if !MatchResult(wr, rr) {
		t.Errorf("Directory %+v not restored successfully, wr:%+v/rr:%+v", meta, wr, rr)
	}
	check(test.Diff(tdir, meta.Name+"_restored"))
}
