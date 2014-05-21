package backend

import (
	"fmt"
	"testing"
	"crypto/sha1"
	"crypto/rand"
	"bytes"
	"sort"
	"reflect"
)

type BlobTest struct {
	Hash string
	Data []byte
}

func RandomBlob(content []byte) (*BlobTest) {
	var data []byte
	if content == nil {
		data = make([]byte, 512)
		rand.Read(data)
	} else {
		data = content
	}
	sha := sha1.New()
	sha.Write(data)
	hash := fmt.Sprintf("%x", sha.Sum(nil))
	return &BlobTest{hash, data}
}

func Test(t *testing.T, b BlobHandler) {
	t.Logf("Testing backend %T", b)
	blobs := []*BlobTest{RandomBlob([]byte("foo")), RandomBlob([]byte("testblob")),
		RandomBlob([]byte("0000")), RandomBlob(nil)}
	
	if !testing.Short() {
		for i := 0; i < 50; i++ {
			blobs = append(blobs, RandomBlob(nil))
		}
	}
	eblobs := []string{}
	for _, blob := range blobs {
		eblobs = append(eblobs, blob.Hash)
	}
	sort.Strings(eblobs)
	t.Logf("%v test blobs generated", len(blobs))

	t.Logf("Test empty enumerate")
	rblobs := []string{}
	cblobs := make(chan string)
	go b.Enumerate(cblobs)
	for blobHash := range cblobs {
		rblobs = append(rblobs, blobHash)
	}
	if len(rblobs) != 0 {
		t.Fatalf("Enumerate should return nothing, got: %q", rblobs)
	}

	t.Logf("Testing Put")

	for i, blob := range blobs {
		if err := b.Put(blob.Hash, blob.Data); err != nil {
			t.Fatalf(fmt.Sprintf("Error put blob #%v %+v: %v", i, blob, err))
		}
	}

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

	t.Logf("Testing Get non-existent")
	neBlob, err := b.Get("d9fb9b3717dbf4cf657b503c0a4f42469309359a")
	if neBlob != nil || err == nil {
		t.Fatalf("No blob should be returned")	
	}

	t.Logf("Testing Enumerate")

	rblobs = []string{}
	cblobs = make(chan string)
	go b.Enumerate(cblobs)
	for blobHash := range cblobs {
		rblobs = append(rblobs, blobHash)
	}
	sort.Strings(rblobs)
	if !reflect.DeepEqual(eblobs, rblobs) {
		t.Fatalf("Error enumerate blobs, got %v, expected %v", rblobs, eblobs)
	}
}