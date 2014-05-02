package models

import (
	"testing"
	"os"
	"fmt"
	"io/ioutil"
	"bytes"
	"log"
)

func TestClientFile(t *testing.T) {
	c, err := NewClient()
 	check(err)
	rfile := NewRandomFile(".")
	defer os.Remove(rfile)
	th := FullSHA1(rfile)
	_, h, err := c.PutFile(rfile)
	check(err)
	if h.Hash != th {
		t.Errorf("File not put successfully")
	}

	rfile2 := fmt.Sprintf("%v%v", rfile, "_restored")
	_, err = c.GetFile(h.Hash, rfile2)
	check(err)

	h2 := FullSHA1(rfile2)
	defer os.Remove(rfile2)
	if th != h2 {
		t.Errorf("File not restored successfully, hash:%v restored hash:%v", th, h2)
	}
	//if !MatchResult(h, rr) {
	//	t.Errorf("File not restored successfully, wr:%+v/rr:%+v", h, rr)
	//}

	// TODO(tsileo) found a way to check that directories are equals

 	d1 := []byte("hello world\n")
 	helloPath := "test_hello_world.txt"
    err = ioutil.WriteFile(helloPath, d1, 0644)
    check(err)
    defer os.Remove(helloPath)
    _, rw, err := c.PutFile(helloPath)
    check(err)
    log.Printf("fileput hash: %v", rw.Hash)
    fakeFile := NewFakeFile(c, rw.Hash, rw.Size)
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
