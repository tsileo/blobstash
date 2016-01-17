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
	"net/http"
	"sync"

	"github.com/tsileo/blobstash/httputil"
	serverMiddleware "github.com/tsileo/blobstash/middleware"
	"github.com/tsileo/blobstash/nsdb"

	"github.com/dchest/blake2b"
	"github.com/gorilla/mux"
	log2 "gopkg.in/inconshreveable/log15.v2"
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
	nsdb *nsdb.DB
	log  log2.Logger
}

func New(ns *nsdb.DB, logger log2.Logger) *SyncTable {
	return &SyncTable{
		nsdb: ns,
		log:  logger,
	}
}
func (st *SyncTable) RegisterRoute(r *mux.Router, middlewares *serverMiddleware.SharedMiddleware) {
	r.Handle("/_state/{ns}", middlewares.Auth(http.HandlerFunc(st.stateHandler())))
	r.Handle("/_state/{ns}/leafs/{prefix}", middlewares.Auth(http.HandlerFunc(st.stateLeafsHandler())))
}

func (st *SyncTable) stateHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		state := NewStateTree()
		vars := mux.Vars(r)
		ns := vars["ns"]
		st.log.Info("_state called", "ns", ns)
		hashes, err := st.nsdb.Namespace(ns, "")
		if err != nil {
			panic(err)
		}
		for _, h := range hashes {
			st.log.Debug("_state loop", "ns", ns, "hash", h)
			state.Add(h)
		}
		httputil.WriteJSON(w, map[string]interface{}{
			"namespace": ns,
			"root":      state.Root(),
			"count":     state.Count(),
			"leafs":     state.Level1(),
		})
	}
}

func (st *SyncTable) stateLeafsHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		ns := vars["ns"]
		prefix := vars["prefix"]
		st.log.Info("_state/leafs called", "ns", ns, "prefix", prefix)
		hashes, err := st.nsdb.Namespace(ns, prefix)
		if err != nil {
			panic(err)
		}
		httputil.WriteJSON(w, map[string]interface{}{
			"namespace": ns,
			"prefix":    prefix,
			"count":     len(hashes),
			"hashes":    hashes,
		})
	}
}

type StateTree struct {
	root   hash.Hash
	level1 map[string]hash.Hash

	count int

	sync.Mutex
}

func NewStateTree() *StateTree {
	return &StateTree{
		root:   blake2b.New256(),
		level1: map[string]hash.Hash{},
	}
}

func (st *StateTree) Close() error {
	hashPool.Put(st.root)
	st.root = nil
	for _, h := range st.level1 {
		hashPool.Put(h)
	}
	st.level1 = nil
	return nil
}

func (st *StateTree) Root() string {
	st.Lock()
	defer st.Unlock()
	return fmt.Sprintf("%x", st.root.Sum(nil))
}

func (st *StateTree) Level1Prefix(prefix string) string {
	st.Lock()
	defer st.Unlock()
	return fmt.Sprintf("%x", st.level1[prefix].Sum(nil))
}

func (st *StateTree) Level1() map[string]interface{} {
	st.Lock()
	defer st.Unlock()
	res := map[string]interface{}{}
	for k, h := range st.level1 {
		res[k] = fmt.Sprintf("%x", h.Sum(nil))
	}
	return res
}

func (st *StateTree) Add(h string) {
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
	st.count++
}
func (st *StateTree) Count() int {
	return st.count
}

// TODO(tsileo): add sync endpoints
