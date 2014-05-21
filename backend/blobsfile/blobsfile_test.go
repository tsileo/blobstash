package blobsfile

import (
	"log"
	"testing"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func TestBlobsFileBackend(t *testing.T) {
	backend := NewBlobsFileBackend("/box/tmp_blobsfile_test")
	//check(err)
	defer backend.Close()
	defer backend.Remove()
	err := backend.load()
	check(err)
	log.Printf("%+v", backend)
}
