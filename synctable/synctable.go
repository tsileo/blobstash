/*

Package synctable implements a sync mechanism using Merkle trees (tree of hahes) for partial sync of blobs namespaces.

The algorithm is inspired by Dynamo or Cassandra uses of Merkle trees (as an anti-entropy mechanism).

Each node maintains its own Merkle tree, when doing a sync, the hashes of the tree are checked against each other starting
from the root hash to the leafs.

This first implementation only keep 256 (16**2) buckets (the first 2 hex of the hashes).

Blake2B (the same hashing algorithm used by the Blob Store) is used to compute the tree.

*/
package synctable

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

type SyncTable struct {
	root   hash.Hash
	level1 map[string]hash.Hash

	sync.Mutex
}

func New() *SyncTable {
	return &SyncTable{
		root:   blake2b.New256(),
		level1: map[string]hash.Hash{},
	}
}

func (st *SyncTable) Close() error {
	hashPool.Put(st.root)
	st.root = nil
	for _, h := range st.level1 {
		hashPool.Put(h)
	}
	st.level1 = nil
	return nil
}

func (st *SyncTable) Root() string {
	st.Lock()
	defer st.Unlock()
	return fmt.Sprintf("%x", st.root.Sum(nil))
}

func (st *SyncTable) Level1(prefix string) string {
	st.Lock()
	defer st.Unlock()
	return fmt.Sprintf("%x", st.level1[prefix].Sum(nil))
}

func (st *SyncTable) Add(h string) {
	st.Lock()
	defer st.Unlock()
	var chash hash.Hash
	if exhash, ok := st.level1[h[0:2]]; ok {
		chash = exhash
	} else {
		chash = blake2b.New256()
		st.level1[h[0:2]] = chash
	}
	chash.Write([]byte(h))
	st.root.Write([]byte(h))
}

// TODO(tsileo): add sync endpoints
