/*

Package sync implements a sync mechanism using Merkle trees (tree of hahes) for a two-way sync between two BlobStash instances.

The algorithm is inspired by Dynamo or Cassandra uses of Merkle trees (as an anti-entropy mechanism).

Each node maintains its own Merkle tree, when doing a sync, the hashes of the tree are checked against each other starting from the root hash to the leaves.

This first implementation only keep 256 (16**2) buckets (the first 2 hex of the hashes).

Blake2B (the same hashing algorithm used by the Blob Store) is used to compute the tree.

*/
package sync // import "a4.io/blobstash/pkg/sync"

import (
	"context"
	"fmt"
	"hash"
	"net/http"
	"sync"

	"a4.io/blobstash/pkg/blobstore"
	"a4.io/blobstash/pkg/config"
	"a4.io/blobstash/pkg/httputil"

	"github.com/dchest/blake2b"
	"github.com/gorilla/mux"
	log2 "github.com/inconshreveable/log15"
	logext "github.com/inconshreveable/log15/ext"
)

// FIXME(tsileo): ensure the keys/maps are sorted/iterated in lexicographical order

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

type Sync struct {
	blobstore *blobstore.BlobStore
	conf      *config.Config

	log log2.Logger
}

func New(logger log2.Logger, conf *config.Config, blobstore *blobstore.BlobStore) *Sync {
	logger.Debug("init")
	return &Sync{
		blobstore: blobstore,
		conf:      conf,
		log:       logger,
	}
}

func (st *Sync) Register(r *mux.Router, basicAuth func(http.Handler) http.Handler) {
	r.Handle("/state", basicAuth(http.HandlerFunc(st.stateHandler())))
	r.Handle("/state/leaf/{prefix}", basicAuth(http.HandlerFunc(st.stateLeafHandler())))
	r.Handle("/_trigger", basicAuth(http.HandlerFunc(st.triggerHandler())))
}

func (st *Sync) Sync(url, apiKey string) (*SyncStats, error) {
	log := st.log.New("trigger_id", logext.RandId(6))
	log.Info("Starting sync...", "url", url)
	rawState := st.generateTree()
	defer rawState.Close()
	client := NewSyncClient(st.log.New("submodule", "synctable-client"), st, rawState, st.blobstore, url, apiKey)
	return client.Sync()
}

func (st *Sync) triggerHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		url := q.Get("url")
		apiKey := q.Get("api_key")
		stats, err := st.Sync(url, apiKey)
		if err != nil {
			panic(err)
		}
		httputil.WriteJSON(w, stats)
	}
}

func (st *Sync) generateTree() *StateTree {
	state := NewStateTree()
	blobs, err := st.blobstore.Enumerate(context.Background(), "", "\xff", 0)
	if err != nil {
		panic(err)
	}
	for _, blob := range blobs {
		// st.log.Debug("_state loop", "ns", ns, "hash", h)
		state.Add(blob.Hash)
	}
	return state
}

func (st *Sync) stateHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		state := st.generateTree()
		defer state.Close()
		httputil.WriteJSON(w, state.State())
	}
}

type State struct {
	Root   string            `json:"root"`
	Count  int               `json:"count"`
	Leaves map[string]string `json:"leaves"`
}

func (st *State) String() string {
	return fmt.Sprintf("[State root=%s, hashes_cnt=%v, leaves_cnt=%v]", st.Root, st.Count, len(st.Leaves))
}

func (st *Sync) LeafState(prefix string) (*LeafState, error) {
	blobs, err := st.blobstore.Enumerate(context.Background(), prefix, prefix+"\xff", 0)
	if err != nil {
		panic(err)
	}
	var hashes []string
	for _, blob := range blobs {
		// st.log.Debug("_state loop", "ns", ns, "hash", h)
		hashes = append(hashes, blob.Hash)
	}

	return &LeafState{
		Prefix: prefix,
		Count:  len(hashes),
		Hashes: hashes,
	}, nil
}

func (st *Sync) stateLeafHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		prefix := vars["prefix"]
		leafState, err := st.LeafState(prefix)
		if err != nil {
			panic(err)
		}
		httputil.WriteJSON(w, leafState)
	}
}

type LeafState struct {
	Prefix string   `json:"prefix"`
	Count  int      `json:"count"`
	Hashes []string `json:"hashes"`
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

func (st *StateTree) String() string {
	return fmt.Sprintf("[StateTree root=%s, hashes_cnt=%v, leaves_cnt=%v]", st.Root(), st.Count(), len(st.level1))
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
	if h, ok := st.level1[prefix]; ok {
		return fmt.Sprintf("%x", h.Sum(nil))
	}
	return ""
}

func (st *StateTree) Level1() map[string]string {
	st.Lock()
	defer st.Unlock()
	res := map[string]string{}
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

func (st *StateTree) State() *State {
	return &State{
		Root:   st.Root(),
		Count:  st.Count(),
		Leaves: st.Level1(),
	}
}

// TODO(tsileo): import the scheduler from blobsnap to run sync periodically
