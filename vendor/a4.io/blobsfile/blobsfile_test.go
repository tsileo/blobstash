package blobsfile

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io/ioutil"
	mrand "math/rand"
	"os"
	"reflect"
	"sort"
	"testing"

	"a4.io/blobstash/pkg/hashutil"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func BenchmarkBlobsFilePut512B(b *testing.B) {
	back, err := New(&Opts{Directory: "./tmp_blobsfile_test", DisableCompression: true})
	check(err)
	defer back.Close()
	defer os.RemoveAll("./tmp_blobsfile_test")
	benchmarkBlobsFilePut(back, 512, b)
}

func BenchmarkBlobsFilePut512KB(b *testing.B) {
	back, err := New(&Opts{Directory: "./tmp_blobsfile_test", DisableCompression: true})
	check(err)
	defer back.Close()
	defer os.RemoveAll("./tmp_blobsfile_test")
	benchmarkBlobsFilePut(back, 512000, b)
}

func BenchmarkBlobsFilePut2MB(b *testing.B) {
	back, err := New(&Opts{Directory: "./tmp_blobsfile_test", DisableCompression: true})
	check(err)
	defer back.Close()
	defer os.RemoveAll("./tmp_blobsfile_test")
	benchmarkBlobsFilePut(back, 2000000, b)
}

func BenchmarkBlobsFilePut512BCompressed(b *testing.B) {
	back, err := New(&Opts{Directory: "./tmp_blobsfile_test"})
	check(err)
	defer back.Close()
	defer os.RemoveAll("./tmp_blobsfile_test")
	benchmarkBlobsFilePut(back, 512, b)
}

func BenchmarkBlobsFilePut512KBCompressed(b *testing.B) {
	back, err := New(&Opts{Directory: "./tmp_blobsfile_test"})
	check(err)
	defer back.Close()
	defer os.RemoveAll("./tmp_blobsfile_test")
	benchmarkBlobsFilePut(back, 512000, b)
}

func BenchmarkBlobsFilePut2MBCompressed(b *testing.B) {
	back, err := New(&Opts{Directory: "./tmp_blobsfile_test"})
	check(err)
	defer back.Close()
	defer os.RemoveAll("./tmp_blobsfile_test")
	benchmarkBlobsFilePut(back, 2000000, b)
}

func benchmarkBlobsFilePut(back *BlobsFiles, blobSize int, b *testing.B) {
	// b.ResetTimer()
	// b.StopTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		h, blob := randBlob(blobSize)
		b.StartTimer()
		if err := back.Put(h, blob); err != nil {
			panic(err)
		}
		b.StopTimer()
	}
	b.SetBytes(int64(blobSize))
}

func TestBlobsFileReedSolomon(t *testing.T) {
	b, err := New(&Opts{Directory: "./tmp_blobsfile_test", DisableCompression: true, BlobsFileSize: 16000000})
	check(err)
	defer os.RemoveAll("./tmp_blobsfile_test")
	testParity(t, b, true, nil)
	fname := b.filename(0)
	b.Close()
	// // Corrupt the file

	// f, err := os.OpenFile(fname, os.O_RDWR, 0755)
	// if err != nil {
	// 	panic(err)
	// }
	// FIXME(tsileo): test this
	// if _, err := f.Seek(defaultMaxBlobsFileSize/10*3, os.SEEK_SET); err != nil {
	// if _, err := f.Seek(defaultMaxBlobsFileSize/10, os.SEEK_SET); err != nil {
	// if _, err := f.Seek(16000000/10*2, os.SEEK_SET); err != nil {
	data, err := ioutil.ReadFile(fname)
	if err != nil {
		panic(err)
	}
	punchOffset := int64(16000000/10*5) - 10
	t.Logf("punch at %d\n", punchOffset)
	fmt.Printf("punch at %d/%d\n", punchOffset, 16000000)
	ndata := []byte("blobsfilelol")
	copy(data[punchOffset:punchOffset+int64(len(ndata))], ndata)
	if err := ioutil.WriteFile(fname, []byte(data), 0644); err != nil {
		panic(err)
	}
	// Reopen the db
	b, err = New(&Opts{Directory: "./tmp_blobsfile_test", DisableCompression: true, BlobsFileSize: 16000000})
	check(err)
	defer b.Close()
	// Ensure we can recover from this corruption
	cb := func(err error) error {
		if err != nil {
			if err := b.scan(nil); err != nil {
				return b.checkBlobsFile(err.(*corruptedError))
			}
			panic("should not happen")
		}
		return nil
	}
	testParity(t, b, false, cb)
}

func TestBlobsFileReedSolomonReindex(t *testing.T) {
	b, err := New(&Opts{Directory: "./tmp_blobsfile_test", DisableCompression: true, BlobsFileSize: 16000000})
	check(err)
	defer os.RemoveAll("./tmp_blobsfile_test")
	testParity(t, b, true, nil)
	fname := b.filename(0)
	b.Close()
	// // Corrupt the file

	// f, err := os.OpenFile(fname, os.O_RDWR, 0755)
	// if err != nil {
	// 	panic(err)
	// }
	// FIXME(tsileo): test this
	// if _, err := f.Seek(defaultMaxBlobsFileSize/10*3, os.SEEK_SET); err != nil {
	// if _, err := f.Seek(defaultMaxBlobsFileSize/10, os.SEEK_SET); err != nil {
	// if _, err := f.Seek(16000000/10*2, os.SEEK_SET); err != nil {
	data, err := ioutil.ReadFile(fname)
	if err != nil {
		panic(err)
	}
	punchOffset := int64(16000000/10*5) - 10
	t.Logf("punch at %d\n", punchOffset)
	fmt.Printf("punch at %d/%d\n", punchOffset, 16000000)
	ndata := []byte("blobsfilelol")
	copy(data[punchOffset:punchOffset+int64(len(ndata))], ndata)
	if err := ioutil.WriteFile(fname, []byte(data), 0644); err != nil {
		panic(err)
	}
	// Reopen the db
	b, err = New(&Opts{Directory: "./tmp_blobsfile_test", DisableCompression: true, BlobsFileSize: 16000000})
	check(err)
	defer b.Close()
	if err := b.RebuildIndex(); err != nil {
		t.Errorf("failed to rebuild index: %v", err)
	}
}

func TestBlobsFileReedSolomonWithCompression(t *testing.T) {
	b, err := New(&Opts{Directory: "./tmp_blobsfile_test", DisableCompression: true, BlobsFileSize: 16000000})
	check(err)
	defer b.Close()
	defer os.RemoveAll("./tmp_blobsfile_test")
	testParity(t, b, true, nil)
}

func testParity(t *testing.T, b *BlobsFiles, insert bool, cb func(error) error) ([]string, [][]byte) {
	hashes := []string{}
	blobs := [][]byte{}
	if insert {
		for i := 0; i < 31+10; i++ {
			h, blob := randBlob(512000)
			hashes = append(hashes, h)
			blobs = append(blobs, blob)
			if err := b.Put(h, blob); err != nil {
				panic(err)
			}
		}
	}
	if err := b.checkParityBlobs(0); err != nil {
		if cb == nil {
			panic(err)
		}
		if err := cb(err); err != nil {
			panic(err)
		}
	}
	return hashes, blobs
}

func randBlob(size int) (string, []byte) {
	blob := make([]byte, size)
	if _, err := rand.Read(blob); err != nil {
		panic(err)
	}
	return hashutil.Compute(blob), blob
}

func TestBlobsFilePutIdempotent(t *testing.T) {
	back, err := New(&Opts{Directory: "./tmp_blobsfile_test"})
	check(err)
	defer back.Close()
	defer os.RemoveAll("./tmp_blobsfile_test")
	h, blob := randBlob(512)
	for i := 0; i < 10; i++ {
		if err := back.Put(h, blob); err != nil {
			panic(err)
		}
	}
	stats, err := back.Stats()
	if err != nil {
		panic(err)
	}
	if stats.BlobsCount != 1 || stats.BlobsSize != 512 {
		t.Errorf("bad stats: %+v", stats)
	}
}

func TestBlobsFileBlobPutGetEnumerate(t *testing.T) {
	b, err := New(&Opts{Directory: "./tmp_blobsfile_test", DisableCompression: true})
	check(err)
	defer os.RemoveAll("./tmp_blobsfile_test")
	hashes, blobs := testBackendPutGetEnumerateReindexGetEnumerate(t, b, 500)
	b.Close()
	// Test we can still read everything when closing/reopening the blobsfile
	b, err = New(&Opts{Directory: "./tmp_blobsfile_test"})
	check(err)
	prefixes := map[string][]string{}
	for _, h := range hashes {
		if _, ok := prefixes[h[0:2]]; !ok {
			prefixes[h[0:2]] = []string{}
		}
		prefixes[h[0:2]] = append(prefixes[h[0:2]], h)
	}
	testBackendEnumerate(t, b, hashes, "", "\xff")
	for prefix, phashes := range prefixes {
		testBackendEnumerate(t, b, phashes, prefix, prefix+"\xff")
	}
	testBackendGet(t, b, hashes, blobs)
	if err := b.Close(); err != nil {
		panic(err)
	}
	// Try with the index and removed and test re-indexing
	b, err = New(&Opts{Directory: "./tmp_blobsfile_test"})
	check(err)
	if err := b.RebuildIndex(); err != nil {
		panic(err)
	}
	testBackendEnumerate(t, b, hashes, "", "\xff")
	testBackendGet(t, b, hashes, blobs)
}

func backendPut(t *testing.T, b *BlobsFiles, blobsCount int) ([]string, [][]byte) {
	blobs := [][]byte{}
	hashes := []string{}
	// TODO(tsileo): 50 blobs if in short mode
	for i := 0; i < blobsCount; i++ {
		h, blob := randBlob(mrand.Intn(4000000-32) + 32)
		hashes = append(hashes, h)
		blobs = append(blobs, blob)
		if err := b.Put(h, blob); err != nil {
			panic(err)
		}
	}

	stats, err := b.Stats()
	if err != nil {
		panic(err)
	}
	fmt.Printf("stats=%+v\n", stats)

	return hashes, blobs
}

func testBackendPutGetEnumerate(t *testing.T, b *BlobsFiles, blobsCount int) ([]string, [][]byte) {
	hashes, blobs := backendPut(t, b, blobsCount)
	testBackendGet(t, b, hashes, blobs)
	testBackendEnumerate(t, b, hashes, "", "\xff")
	return hashes, blobs
}

func testBackendPutGetEnumerateReindexGetEnumerate(t *testing.T, b *BlobsFiles, blobsCount int) ([]string, [][]byte) {
	hashes, blobs := backendPut(t, b, blobsCount)
	testBackendGet(t, b, hashes, blobs)
	testBackendEnumerate(t, b, hashes, "", "\xff")
	if err := b.RebuildIndex(); err != nil {
		panic(err)
	}
	testBackendGet(t, b, hashes, blobs)
	testBackendEnumerate(t, b, hashes, "", "\xff")
	return hashes, blobs
}

func testBackendGet(t *testing.T, b *BlobsFiles, hashes []string, blobs [][]byte) {
	blobsIndex := map[string]bool{}
	for _, blob := range blobs {
		blobsIndex[hashutil.Compute(blob)] = true
	}
	for _, h := range hashes {
		if _, err := b.Get(h); err != nil {
			panic(err)
		}
		_, ok := blobsIndex[h]
		if !ok {
			t.Errorf("blob %s should be index", h)
		}
		delete(blobsIndex, h)
	}
	if len(blobsIndex) > 0 {
		t.Errorf("index should have been emptied, got len %d", len(blobsIndex))
	}
}

func testBackendEnumerate(t *testing.T, b *BlobsFiles, hashes []string, start, end string) []string {
	sort.Strings(hashes)
	bchan := make(chan *Blob)
	errc := make(chan error, 1)
	go func() {
		errc <- b.Enumerate(bchan, start, end, 0)
	}()
	enumHashes := []string{}
	for ref := range bchan {
		enumHashes = append(enumHashes, ref.Hash)
	}
	if err := <-errc; err != nil {
		panic(err)
	}
	if !sort.StringsAreSorted(enumHashes) {
		t.Errorf("enum hashes should already be sorted")
	}
	if !reflect.DeepEqual(hashes, enumHashes) {
		t.Errorf("bad enumerate results")
	}
	return enumHashes
}

func TestBlobsFileBlobEncodingNoCompression(t *testing.T) {
	b, err := New(&Opts{Directory: "./tmp_blobsfile_test", DisableCompression: true})
	check(err)
	defer b.Close()
	defer os.RemoveAll("./tmp_blobsfile_test")
	_, blob := randBlob(512)
	_, data := b.encodeBlob(blob, flagBlob)
	size, blob2, f := b.decodeBlob(data)
	if f != flagBlob {
		t.Errorf("bad flag, got %v, expected %v", f, flagBlob)
	}
	if size != 512 || !bytes.Equal(blob, blob2) {
		t.Errorf("Error blob encoding, got size:%v, expected:512, got blob:%v, expected:%v", size, blob2[:10], blob[:10])
	}
}

func TestBlobsFileBlobEncoding(t *testing.T) {
	b, err := New(&Opts{Directory: "./tmp_blobsfile_test"})
	check(err)
	defer b.Close()
	defer os.RemoveAll("./tmp_blobsfile_test")
	_, blob := randBlob(512)
	_, data := b.encodeBlob(blob, flagBlob)
	size, blob2, f := b.decodeBlob(data)
	if f != flagBlob {
		t.Errorf("bad flag, got %v, expected %v", f, flagBlob)
	}
	// Don't check the size are as the returned size is the size of the compressed blob
	if !bytes.Equal(blob, blob2) {
		t.Errorf("Error blob encoding, got size:%v, expected:512, got blob:%v, expected:%v", size, blob2[:10], blob[:10])
	}
}
