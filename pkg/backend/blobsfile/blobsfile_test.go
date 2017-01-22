package blobsfile

import (
	"bytes"
	"crypto/rand"
	"os"
	"sync"
	"testing"

	_ "a4.io/blobstash/pkg/backend"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

//func TestBlobsFileBackendWithCompression(t *testing.T) {
//	b := New("./tmp_blobsfile_test_compressed", 0, true, false)
//	//check(err)
//	defer b.Close()
//	defer os.RemoveAll("./tmp_blobsfile_test_compressed")
//	backend.Test(t, b)
//}

//func TestBlobsFileBackend(t *testing.T) {
//	b := New("./tmp_blobsfile_test", 0, false, false)
//	//check(err)
//	defer b.Close()
//	defer os.RemoveAll("./tmp_blobsfile_test")
//	backend.Test(t, b)
//}

//func TestBlobsFileBackendCloseAndReopen(t *testing.T) {
//	b := New("./tmp_blobsfile_test_reopen", 0, false, false)
//	//check(err)
//	defer os.RemoveAll("./tmp_blobsfile_test_reopen")
//	backend.Test(t, b)
//	b.Close()
//	b2 := New("./tmp_blobsfile_test_reopen", 0, false, false)
//	backend.TestReadOnly(t, b2)
//}

//func TestBlobsFileBackendCloseAndReopenAndReindexCompressed(t *testing.T) {
//	b := New("./tmp_blobsfile_test_reindexc", 0, true, false)
//	//check(err)
//	defer os.RemoveAll("./tmp_blobsfile_test_reindexc")
//	backend.Test(t, b)
//	b.Close()
//	b.Remove()
//	b2 := New("./tmp_blobsfile_test_reindexc", 0, true, false)
//	backend.TestReadOnly(t, b2)
//}

//func TestBlobsFileBackendCloseAndReopenAndReindex(t *testing.T) {
//	b := New("./tmp_blobsfile_test_reindex", 0, false, false)
//	//check(err)
//	defer os.RemoveAll("./tmp_blobsfile_test_reindex")
//	backend.Test(t, b)
//	b.Close()
//	b.Remove()
//	b2 := New("./tmp_blobsfile_test_reindex", 0, false, false)
//	backend.TestReadOnly(t, b2)
//}

//func TestBlobsFileBackendWithSmallMaxBlobsFileSize(t *testing.T) {
//	b := New("./tmp_blobsfile_test_small", 512, false, false)
//	//check(err)
//	defer b.Close()
//	defer os.RemoveAll("./tmp_blobsfile_test_small")
//	backend.Test(t, b)
//}

//func TestBlobsFileBackendWriteOnly(t *testing.T) {
//	b := New("./tmp_blobsfile_test_wo", 0, false, true)
//	//check(err)
//	defer b.Close()
//	defer os.RemoveAll("./tmp_blobsfile_test_wo")
//	backend.TestWriteOnly(t, b)
//}

//func TestBlobsFileBackendWriteOnlyWithSmallMaxBlobsFileSize(t *testing.T) {
//	b := New("./tmp_blobsfile_test_wo_small", 512, false, true)
//	//check(err)
//	defer b.Close()
//	defer os.RemoveAll("./tmp_blobsfile_test_wo_small")
//	backend.TestWriteOnly(t, b)
//}

func TestBlobsFileBlobEncoding(t *testing.T) {
	b := New("./tmp_blobsfile_test", 0, false, sync.WaitGroup{})
	//check(err)
	defer b.Close()
	defer os.RemoveAll("./tmp_blobsfile_test")
	blob := make([]byte, 512)
	rand.Read(blob)
	_, data := b.encodeBlob(blob)
	size, blob2 := b.decodeBlob(data)
	if size != 512 || !bytes.Equal(blob, blob2) {
		t.Errorf("Error blob encoding, got size:%v, expected:512, got blob:%v, expected:%v", size, blob2[:10], blob[:10])
	}
}
