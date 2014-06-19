package client

import (
	"os"
	"testing"
	"reflect"

	"github.com/tsileo/blobstash/test"
)

func TestClient(t *testing.T) {
	s, err := test.NewTestServer(t)
	check(err)
	go s.Start()
	if err := s.TillReady(); err != nil {
		t.Fatalf("server error:\n%v", err)
	}
	defer s.Shutdown()

	c, err := NewTestClient("")
	defer c.Close()
	defer c.RemoveCache()
	check(err)
	tdir := test.NewRandomTree(t, ".", 1)
	defer os.RemoveAll(tdir)
	putSnap, _, _, err := c.Put(tdir)
	check(err)

	t.Log("Testing client.Hosts()")

	hostname, err := os.Hostname()
	check(err)
	hosts, err := c.Hosts()
	check(err)
	if len(hosts) != 1 || hosts[0] != hostname {
		t.Errorf("Hosts() should return [%v], got %q", hostname, hosts)
	}

	t.Logf("Testing client.Backups(%v)", hostname)

	backups, err := c.Backups(hostname)
	check(err)
	if len(backups) != 1 {
		t.Errorf("Backups len should be 1, got %v: %q", len(backups), backups)
	}

	t.Logf("Testing backup.Snapshots()")

	backup := backups[0]
	snapshots, err := backup.Snapshots()
	snap := snapshots[0].Snapshot
	check(err)
	if len(snapshots) != 1 || !reflect.DeepEqual(putSnap, snap) {
		t.Errorf("Snapshots len should be 1, got %v and: %q", len(snapshots), snapshots)
	}

	t.Logf("Testing backup.Last()")

	snap2, err := backup.Last()
	check(err)

	if !reflect.DeepEqual(snap, snap2) {
		t.Errorf("backup.Last() expected %+v, got %+v", snap, snap2)
	}

	// t.Logf("Testing backup.GetAt(%v)", ts)
	// TODO test backup.GetAt

	//rr, err := c.GetDir(meta.Hash, meta.Name+"_restored")
	//defer os.RemoveAll(meta.Name+"_restored")
	//check(err)
	//if !MatchResult(wr, rr) {
	//	t.Errorf("Directory %+v not restored successfully, wr:%+v/rr:%+v", meta, wr, rr)
	//}
	//check(test.Diff(tdir, meta.Name+"_restored"))
}
