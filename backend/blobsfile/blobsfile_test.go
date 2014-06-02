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

func TestBlobsFileBackendWithCompression(t *testing.T) {
	b := New("./tmp_blobsfile_test_compressed", true)
	//check(err)
	defer b.Close()
	defer os.RemoveAll("./tmp_blobsfile_test_compressed")
	backend.Test(t, b)
}

func TestBlobsFileBackend(t *testing.T) {
	b := New("./tmp_blobsfile_test", false)
	//check(err)
	defer b.Close()
	defer os.RemoveAll("./tmp_blobsfile_test")
	backend.Test(t, b)
}

func TestBlobsFileBlobEncoding(t *testing.T) {
	b := New("./tmp_blobsfile_test", false)
	//check(err)
	defer b.Close()
	defer os.RemoveAll("./tmp_blobsfile_test")
	blob := make([]byte, 512)
	rand.Read(blob)
	data := b.encodeBlob(len(blob), blob)
	size, blob2 := b.decodeBlob(data)
	if size != 512 || !bytes.Equal(blob, blob2) {
		t.Errorf("Error blob encoding, got size:%v, expected:512, got blob:%v, expected:%v", size, blob2[:10], blob[:10])
	}
}
