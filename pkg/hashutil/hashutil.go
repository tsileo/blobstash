package hashutil

import (
	"fmt"
	"hash"
	"sync"

	"github.com/dchest/blake2b"
)

var hashPool sync.Pool

func NewHash() (h hash.Hash) {
	if ih := hashPool.Get(); ih != nil {
		h = ih.(hash.Hash)
		h.Reset()
	} else {
		// Creates a new one if the pool is empty
		h = blake2b.New256()
	}
	return
}

// Compute returns the Blake2B hash hex-encoded
func Compute(data []byte) string {
	h := NewHash()
	hash := fmt.Sprintf("%x", h.Sum(data))
	hashPool.Put(h)
	return hash
}
