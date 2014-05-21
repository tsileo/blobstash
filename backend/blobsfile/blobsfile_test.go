package blobsfile

import (
	"log"
	"testing"
	"bytes"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func TestBlobsFileBackend(t *testing.T) {
	backend := New("/box/tmp_blobsfile_test")
	//check(err)
	defer backend.Close()
	defer backend.Remove()
	err := backend.load()
	check(err)
	log.Printf("%+v", backend)
	err = backend.Put("OMG", []byte("YES"))
	check(err)
	log.Printf("%+v", backend)
	data, err := backend.Get("OMG")
	check(err)
	log.Printf("DATA:%v", string(data))
}

func TestBlobsFileBlobEncoding(t *testing.T) {
	data := encodeBlob(5, []byte("ooooo"))
	size, blob := decodeBlob(data)
	if size != 5 || !bytes.Equal(blob, []byte("ooooo")) {
		t.Errorf("Error blob encoding, got size:%v, expected:5, got blob:%v, expected:%v", size, blob, []byte("ooooo"))
	}
}
