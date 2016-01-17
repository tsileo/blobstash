/*

Package synctable implements a sync mechanism usingi Merkle trees (tree of hahes) for partial sync of blobs namespaces.

*/
package synctable

import (
	"fmt"
	"hash"
	"sync"

	"github.com/dchest/blake2b"
)

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
