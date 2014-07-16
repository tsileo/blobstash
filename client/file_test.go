package client

import (
	"bytes"
	"time"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/tsileo/blobstash/test"
)

func TestClientFile(t *testing.T) {
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
	rfile := test.NewRandomFile(".")
	defer os.Remove(rfile)
	th := FullSHA1(rfile)
	m, h, err := c.PutFile(&Ctx{Namespace: c.Hostname}, nil, rfile)
	check(err)
	if h.Hash != th {
		t.Errorf("File not put successfully")
	}
	time.Sleep(2*time.Second)

	rfile2 := fmt.Sprintf("%v%v", rfile, "_restored")
	rr, err := c.GetFile(&Ctx{Namespace: c.Hostname}, m.Hash, rfile2)
	check(err)

	h2 := FullSHA1(rfile2)
	defer os.Remove(rfile2)
	if th != h2 {
		t.Errorf("File not restored successfully, hash:%v restored hash:%v", th, h2)
	}
	if !MatchResult(h, rr) {
		t.Errorf("File not restored successfully, wr:%+v/rr:%+v", h, rr)
	}

	d1 := []byte("hello world\n")
	helloPath := "test_hello_world.txt"
	err = ioutil.WriteFile(helloPath, d1, 0644)
	check(err)
	defer os.Remove(helloPath)
	_, rw, err := c.PutFile(&Ctx{Namespace: c.Hostname}, nil, helloPath)
	check(err)
	time.Sleep(2*time.Second)
	t.Logf("fileput hash: %v", rw.Hash)
	fakeFile := NewFakeFile(c, &Ctx{Namespace: c.Hostname}, rw.Hash, rw.Size)
	fkr, err := fakeFile.read(0, 5)
	check(err)
	if !bytes.Equal(fkr, []byte("hello")) {
		t.Errorf("Error Fake file read, expected:hello, got %v", fkr)
	}
	fkr, err = fakeFile.read(6, 5)
	check(err)
	if !bytes.Equal(fkr, []byte("world")) {
		t.Errorf("Error Fake file read, expected:world, got %v", fkr)
	}
	d2 := make([]byte, len(d1))
	n, err := fakeFile.Read(d2)
	check(err)
	if n != len(d1) {
		t.Error("Error FakeFile reader len")
	}
	if !bytes.Equal(d1, d2) {
		t.Error("Error FakeFile reader")
	}
}
