package backend

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/dchest/blake2b"
)

type BlobTest struct {
	Hash string
	Data []byte
}

func RandomBlob(content []byte) *BlobTest {
	var data []byte
	if content == nil {
		data = make([]byte, 512)
		rand.Read(data)
	} else {
		data = content
	}
	sha := blake2b.New256()
	sha.Write(data)
	hash := fmt.Sprintf("%x", sha.Sum(nil))
	return &BlobTest{hash, data}
}

func BigRandomBlob() *BlobTest {
	data := make([]byte, 2097152)
	rand.Read(data)
	sha := blake2b.New256()
	sha.Write(data)
	hash := fmt.Sprintf("%x", sha.Sum(nil))
	return &BlobTest{hash, data}
}

func Test(t *testing.T, b BlobHandler) {
	FullTest(t, b, false, false)
}

func TestWriteOnly(t *testing.T, b BlobHandler) {
	FullTest(t, b, true, false)
}

func TestReadOnly(t *testing.T, b BlobHandler) {
	FullTest(t, b, false, true)
}

func FullTest(t *testing.T, b BlobHandler, writeOnlyMode, readOnlyMode bool) {
	t.Logf("Testing backend %T", b)
	blobs := []*BlobTest{RandomBlob([]byte("foo")), RandomBlob([]byte("testblob")),
		RandomBlob([]byte("0000"))}
	if !readOnlyMode {
		blobs = append(blobs, RandomBlob(nil))
	}
	if !testing.Short() && !readOnlyMode {
		for i := 0; i < 50; i++ {
			blobs = append(blobs, RandomBlob(nil))
		}
		blobs = append(blobs, []*BlobTest{BigRandomBlob(), BigRandomBlob()}...)
	}
	eblobs := []string{}
	for _, blob := range blobs {
		eblobs = append(eblobs, blob.Hash)
	}
	sort.Strings(eblobs)
	t.Logf("%v test blobs generated", len(blobs))

	if !readOnlyMode && !writeOnlyMode {
		t.Logf("Test empty enumerate")
		rblobs := []string{}
		cblobs := make(chan string)
		errc := make(chan error)
		go func() {
			errc <- b.Enumerate(cblobs)
		}()
		for blobHash := range cblobs {
			rblobs = append(rblobs, blobHash)
		}
		if err := <-errc; err != nil {
			panic(err)
		}
		if len(rblobs) != 0 {
			t.Fatalf("Enumerate should return nothing, got: %q", rblobs)
		}
	}
	if !readOnlyMode {
		t.Logf("Testing Put")

		for i, blob := range blobs {
			if err := b.Put(blob.Hash, blob.Data); err != nil {
				t.Fatalf(fmt.Sprintf("Error put blob #%v %+v: %v", i, blob, err))
			}
		}
	}
	if !writeOnlyMode {
		t.Logf("Testing Get")

		for i, blob := range blobs {
			blobData, err := b.Get(blob.Hash)
			if err != nil {
				t.Fatalf(fmt.Sprintf("Error get blob %+v: %v", blob, err))
			}
			if !bytes.Equal(blobData, blob.Data) {
				t.Fatalf(fmt.Sprintf("Error get blob #%v %+v data, got %v, expected %v", i, blob, blobData, blob.Data))
			}
		}
	} else {
		t.Logf("Skipping Get in write-only mode")
	}

	t.Logf("Testing Exists")

	if res := b.Exists("d9fb9b3717dbf4cf657b503c0a4f42469309359a"); res {
		t.Fatalf(fmt.Sprintf("Blob %v shouldn't exists", "d9fb9b3717dbf4cf657b503c0a4f42469309359a"))
	}

	if res := b.Exists(eblobs[0]); !res {
		t.Fatalf(fmt.Sprintf("Blob %v should exists", eblobs[0]))
	}

	if !readOnlyMode && !writeOnlyMode {
		t.Logf("Testing Enumerate")

		rblobs := []string{}
		cblobs := make(chan string)
		go b.Enumerate(cblobs)
		for blobHash := range cblobs {
			rblobs = append(rblobs, blobHash)
		}
		sort.Strings(rblobs)
		if !reflect.DeepEqual(eblobs, rblobs) {
			t.Fatalf("Error enumerate blobs, got %v, expected %v", rblobs, eblobs)
		}
	}
}
