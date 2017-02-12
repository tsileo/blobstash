/*

Package synctable implements a sync mechanism using Merkle trees (tree of hahes) for a two-way sync between two BlobStash instances.

The algorithm is inspired by Dynamo or Cassandra uses of Merkle trees (as an anti-entropy mechanism).

Each node maintains its own Merkle tree, when doing a sync, the hashes of the tree are checked against each other starting from the root hash to the leaves.

This first implementation only keep 256 (16**2) buckets (the first 2 hex of the hashes).

Blake2B (the same hashing algorithm used by the Blob Store) is used to compute the tree.

*/
package synctable // import "a4.io/blobstash/pkg/synctable"

import (
	"context"
	"encoding/json"
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

type SyncTable struct {
	blobstore *blobstore.BlobStore
	conf      *config.Config

	log log2.Logger
}

func New(logger log2.Logger, conf *config.Config, blobstore *blobstore.BlobStore) *SyncTable {
	logger.Debug("init")
	return &SyncTable{
		blobstore: blobstore,
		conf:      conf,
		log:       logger,
	}
}

func (st *SyncTable) Register(r *mux.Router, basicAuth func(http.Handler) http.Handler) {
	r.Handle("/state", basicAuth(http.HandlerFunc(st.stateHandler())))
	r.Handle("/state/leaves/{prefix}", basicAuth(http.HandlerFunc(st.stateLeavesHandler())))
	r.Handle("/", basicAuth(http.HandlerFunc(st.syncHandler())))
	r.Handle("/_trigger", basicAuth(http.HandlerFunc(st.triggerHandler())))
}

func (st *SyncTable) triggerHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		log := st.log.New("trigger_id", logext.RandId(6))
		url := q.Get("url")
		log.Info("Starting sync...", "url", url)
		apiKey := q.Get("api_key")
		rawState := st.generateTree()
		defer rawState.Close()
		state := &State{
			Root:   rawState.Root(),
			Count:  rawState.Count(),
			Leaves: rawState.Level1(),
		}
		client := NewSyncTableClient(st.log.New("submodule", "synctable-client"), state, st.blobstore, nil, "", url, apiKey)
		stats, err := client.Sync()
		if err != nil {
			panic(err)
		}
		httputil.WriteJSON(w, stats)
	}
}

func (st *SyncTable) generateTree() *StateTree {
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

func (st *SyncTable) stateHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		state := st.generateTree()
		defer state.Close()
		httputil.WriteJSON(w, map[string]interface{}{
			"root":   state.Root(),
			"count":  state.Count(),
			"leaves": state.Level1(),
		})
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

func (st *SyncTable) stateLeavesHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		prefix := vars["prefix"]
		blobs, err := st.blobstore.Enumerate(context.Background(), prefix, prefix+"\xff", 0)
		if err != nil {
			panic(err)
		}
		var hashes []string
		for _, blob := range blobs {
			// st.log.Debug("_state loop", "ns", ns, "hash", h)
			hashes = append(hashes, blob.Hash)
		}

		st.log.Info("_state/leaves called", "prefix", prefix, "hashes", len(hashes))
		httputil.WriteJSON(w, map[string]interface{}{
			"prefix": prefix,
			"count":  len(hashes),
			"hashes": hashes,
		})
	}
}

type LeafState struct {
	Prefix string   `json:"prefix"`
	Count  int      `json:"count"`
	Hashes []string `json:"hashes"`
}

// FIXME(tsileo): config only works in one way

func (st *SyncTable) syncHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		log := st.log.New("sync_id", logext.RandId(6))
		log.Info("sync triggered")
		state := st.generateTree()
		defer state.Close()
		local_state := &State{
			Root:   state.Root(),
			Leaves: state.Level1(),
			Count:  state.Count(),
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

		// The root differs, found out the leaves we need to inspect
		leavesNeeded := []string{}
		leavesToSend := []string{}
		leavesConflict := []string{}

		for lleaf, lh := range local_state.Leaves {
			if rh, ok := remote_state.Leaves[lleaf]; ok {
				if lh != rh {
					leavesConflict = append(leavesConflict, lleaf)
				}
			} else {
				// This leaf is only present locally, we can send blindly all the blobs belonging to this leaf
				leavesToSend = append(leavesToSend, lleaf)
				// If an entire leaf is missing, this means we can send/receive the entire hashes for the missing leaf
			}
		}
		// Find out the leaves present only on the remote-side
		for rleaf, _ := range remote_state.Leaves {
			if _, ok := local_state.Leaves[rleaf]; !ok {
				leavesNeeded = append(leavesNeeded, rleaf)
			}
		}

		httputil.WriteJSON(w, map[string]interface{}{
			"conflicted": leavesConflict,
			"needed":     leavesNeeded,
			"missing":    leavesToSend,
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

// TODO(tsileo): import the scheduler from blobsnap to run sync periodically
