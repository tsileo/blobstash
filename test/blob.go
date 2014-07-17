package test

import (
	"crypto/rand"
	"crypto/sha1"
	"fmt"
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
	sha := sha1.New()
	sha.Write(data)
	hash := fmt.Sprintf("%x", sha.Sum(nil))
	return &BlobTest{hash, data}
}
