package blobsfile

import (
	"testing"
	"bytes"
	"crypto/rand"
	"os"

	"github.com/tsileo/datadatabase/backend"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func TestBlobsFileBackend(t *testing.T) {
	b := New("./tmp_blobsfile_test")
	//check(err)
	defer b.Close()
	defer os.RemoveAll("./tmp_blobsfile_test")
	backend.Test(t, b)
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
