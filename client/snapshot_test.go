package client

import (
	"reflect"
	"testing"

	"github.com/tsileo/blobstash/test"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func TestModelsSnapshots(t *testing.T) {
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
	// NewSnapshot(hostname, path, type, ref)
	f := NewSnapshot("hostname", "foo", "file", "bar")
	err = f.Save(c.Pool)
	check(err)

	f2, err := NewSnapshotFromDB(c.Pool, f.Hash)
	check(err)
	if !reflect.DeepEqual(f, f2) {
		t.Errorf("Error retrieving file from DB, expected %+v, get %+v", f, f2)
	}
}
