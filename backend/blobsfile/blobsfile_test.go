package blobsfile

import (
	"testing"
	"bytes"
	"crypto/rand"
	"crypto/sha1"
	"fmt"
	"os"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func TestBlobsFileBackend(t *testing.T) {
	backend := New("./tmp_blobsfile_test")
	//check(err)
	defer backend.Close()
	defer os.RemoveAll("./tmp_blobsfile_test")
	hash1 := sha1.New()
	blob1 := make([]byte, 1024)
	rand.Read(blob1)
	hash1.Write(blob1)
	shash1 := fmt.Sprintf("%x", hash1.Sum(nil))
	hash2 := sha1.New()
	blob2 := make([]byte, 512)
	rand.Read(blob2)
	hash2.Write(blob2)
	shash2 := fmt.Sprintf("%x", hash2.Sum(nil))
	
	err := backend.Put(shash1, blob1)
	check(err)
	data, err := backend.Get(shash1)
	check(err)
	if !bytes.Equal(data, blob1) {
		t.Errorf("Error getting blob: %v/%v", data[:10], blob1[:10])
	}

	err = backend.Put(shash2, blob2)
	check(err)
	data, err = backend.Get(shash2)
	check(err)
	if !bytes.Equal(data, blob2) {
		t.Errorf("Error getting blob: %v/%v", data[:10], blob2[:10])
	}	
	
}

func TestBlobsFileBlobEncoding(t *testing.T) {
	blob := make([]byte, 512)
	rand.Read(blob)
	data := encodeBlob(len(blob), blob)
	size, blob2 := decodeBlob(data)
	if size != 512 || !bytes.Equal(blob, blob2) {
		t.Errorf("Error blob encoding, got size:%v, expected:512, got blob:%v, expected:%v", size, blob2[:10], blob[:10])
	}
}
