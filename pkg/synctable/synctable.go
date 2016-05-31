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
	"encoding/json"
	"fmt"
	_ "golang.org/x/net/context"
	"hash"
	"net/http"
	"sync"

	"github.com/tsileo/blobstash/httputil"
	_ "github.com/tsileo/blobstash/pkg/blob"
	"github.com/tsileo/blobstash/pkg/blobstore"
	"github.com/tsileo/blobstash/pkg/config"
	"github.com/tsileo/blobstash/pkg/middleware"
	"github.com/tsileo/blobstash/pkg/nsdb"

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

type SyncTable struct {
	nsdb      *nsdb.DB
	blobstore *blobstore.BlobStore
	conf      *config.Config

	log log2.Logger
}

func New(logger log2.Logger, conf *config.Config, blobstore *blobstore.BlobStore, ns *nsdb.DB) *SyncTable {
	logger.Debug("init")
	return &SyncTable{
		blobstore: blobstore,
		nsdb:      ns,
		conf:      conf,
		log:       logger,
	}
}

func (st *SyncTable) Register(r *mux.Router) {
	r.Handle("/_state/{ns}", middleware.BasicAuth(http.HandlerFunc(st.stateHandler()), st.conf))
	r.Handle("/_state/{ns}/leafs/{prefix}", middleware.BasicAuth(http.HandlerFunc(st.stateLeafsHandler()), st.conf))
	r.Handle("/{ns}", middleware.BasicAuth(http.HandlerFunc(st.syncHandler()), st.conf))
	r.Handle("/_trigger/{ns}", middleware.BasicAuth(http.HandlerFunc(st.triggerHandler()), st.conf))
}

func (st *SyncTable) triggerHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		vars := mux.Vars(r)
		ns := vars["ns"]
		log := st.log.New("trigger_id", logext.RandId(6), "ns", ns)
		url := q.Get("url")
		log.Info("Starting sync...", "url", url)
		apiKey := q.Get("api_key")
		rawState := st.generateTree(ns)
		defer rawState.Close()
		state := &State{
			Namespace: ns,
			Root:      rawState.Root(),
			Count:     rawState.Count(),
			Leafs:     rawState.Level1(),
		}
		client := NewSyncTableClient(st.log.New("submodule", "synctable-client"), state, st.blobstore, st.nsdb, ns, url, apiKey)
		stats, err := client.Sync()
		if err != nil {
			panic(err)
		}
		httputil.WriteJSON(w, stats)
	}
}

func (st *SyncTable) generateTree(ns string) *StateTree {
	state := NewStateTree()
	hashes, err := st.nsdb.Namespace(ns, "")
	if err != nil {
		panic(err)
	}
	for _, h := range hashes {
		// st.log.Debug("_state loop", "ns", ns, "hash", h)
		state.Add(h)
	}
	return state
}

func (st *SyncTable) stateHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		ns := vars["ns"]
		st.log.Info("_state called", "ns", ns)
		state := st.generateTree(ns)
		defer state.Close()
		httputil.WriteJSON(w, map[string]interface{}{
			"namespace": ns,
			"root":      state.Root(),
			"count":     state.Count(),
			"leafs":     state.Level1(),
		})
	}
}

type State struct {
	Namespace string            `json:"namespace"`
	Root      string            `json:"root"`
	Count     int               `json:"count"`
	Leafs     map[string]string `json:"leafs"`
}

func (st *State) String() string {
	return fmt.Sprintf("[State root=%s, hashes_cnt=%v, leafs_cnt=%v]", st.Root, st.Count, len(st.Leafs))
}

func (st *SyncTable) stateLeafsHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		ns := vars["ns"]
		prefix := vars["prefix"]
		hashes, err := st.nsdb.Namespace(ns, prefix)
		if err != nil {
			panic(err)
		}
		st.log.Info("_state/leafs called", "ns", ns, "prefix", prefix, "hashes", len(hashes))
		httputil.WriteJSON(w, map[string]interface{}{
			"namespace": ns,
			"prefix":    prefix,
			"count":     len(hashes),
			"hashes":    hashes,
		})
	}
}

type LeafState struct {
	Namespace string   `json:"namespace"`
	Prefix    string   `json:"prefix"`
	Count     int      `json:"count"`
	Hashes    []string `json:"hashes"`
}

func (st *SyncTable) syncHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		vars := mux.Vars(r)
		ns := vars["ns"]
		log := st.log.New("sync_id", logext.RandId(6), "ns", ns)
		log.Info("sync triggered")
		state := st.generateTree(ns)
		defer state.Close()
		local_state := &State{
			Namespace: ns,
			Root:      state.Root(),
			Leafs:     state.Level1(),
			Count:     state.Count(),
		}
		log.Debug("local state computed", "local_state", local_state.String())
		remote_state := &State{}
		if err := json.NewDecoder(r.Body).Decode(remote_state); err != nil {
			panic(err)
		}
		log.Debug("remote state decoded", "remote_state", remote_state.String())

		// First check the root, if the root hash is the same, then we can't stop here, we are in sync.
		if local_state.Root == remote_state.Root {
			log.Debug("No sync needed")
			w.WriteHeader(http.StatusNoContent)
			return
		}

		// The root differs, found out the leafs we need to inspect
		leafsNeeded := []string{}
		leafsToSend := []string{}
		leafsConflict := []string{}

		for lleaf, lh := range local_state.Leafs {
			if rh, ok := remote_state.Leafs[lleaf]; ok {
				if lh != rh {
					leafsConflict = append(leafsConflict, lleaf)
				}
			} else {
				// This leaf is only present locally, we can send blindly all the blobs belonging to this leaf
				leafsToSend = append(leafsToSend, lleaf)
				// If an entire leaf is missing, this means we can send/receive the entire hashes for the missing leaf
			}
		}
		// Find out the leafs present only on the remote-side
		for rleaf, _ := range remote_state.Leafs {
			if _, ok := local_state.Leafs[rleaf]; !ok {
				leafsNeeded = append(leafsNeeded, rleaf)
			}
		}

		httputil.WriteJSON(w, map[string]interface{}{
			"conflicted": leafsConflict,
			"needed":     leafsNeeded,
			"missing":    leafsToSend,
		})
	}
}

type SyncResp struct {
	Conflicted []string `json:"conflicted"`
	Needed     []string `json:"nedeed"`
	Missing    []string `json:"missing"`
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
	return fmt.Sprintf("[StateTree root=%s, hashes_cnt=%v, leafs_cnt=%v]", st.Root(), st.Count(), len(st.level1))
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

// TODO(tsileo): import the scheduler from blobsnap to run sync periodically
