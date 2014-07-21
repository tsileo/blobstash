package test

import (
	"crypto/rand"
	"fmt"

	"github.com/dchest/blake2b"
)

// BlobTest represents a fake blob for testing purpose.
type BlobTest struct {
	Hash string
	Data []byte
}

// RandomBlob generates a random BlobTest.
func RandomBlob(content []byte) *BlobTest {
	var data []byte
	if content == nil {
		data = make([]byte, 512)
		rand.Read(data)
	} else {
		data = content
	}
	hash := fmt.Sprintf("%x", blake2b.Sum256(data))
	return &BlobTest{hash, data}
}
