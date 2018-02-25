package sync

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"a4.io/blobstash/pkg/blob"
	"a4.io/blobstash/pkg/client/clientutil"
	"a4.io/blobstash/pkg/stash/store"

	log "github.com/inconshreveable/log15"
)

type SyncClient struct {
	client *clientutil.ClientUtil

	blobstore store.BlobStore
	oneWay    bool

	st    *Sync
	state *StateTree

	log log.Logger
}

func NewSyncClient(logger log.Logger, st *Sync, state *StateTree, blobstore store.BlobStore, url, apiKey string, oneWay bool) *SyncClient {
	return &SyncClient{
		client:    clientutil.NewClientUtil(url, clientutil.WithAPIKey(apiKey)),
		st:        st,
		oneWay:    oneWay,
		state:     state,
		blobstore: blobstore,
	}
}

func (stc *SyncClient) RemoteState() (*State, error) {
	s := &State{}
	resp, err := stc.client.Get("/api/sync/state")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if err := clientutil.ExpectStatusCode(resp, http.StatusOK); err != nil {
		return nil, err
	}

	if err := clientutil.Unmarshal(resp, s); err != nil {
		return nil, err
	}
	return s, nil
}

func (stc *SyncClient) RemoteLeaf(prefix string) (*LeafState, error) {
	ls := &LeafState{}
	resp, err := stc.client.Get(fmt.Sprintf("/api/sync/state/leaf/%s", prefix))
	if err != nil {
		return nil, err
	}
	if err := clientutil.ExpectStatusCode(resp, http.StatusOK); err != nil {
		return nil, err
	}

	if err := clientutil.Unmarshal(resp, ls); err != nil {
		return nil, err
	}
	return ls, nil
}

type SyncStats struct {
	Downloaded     int    `json:"blobs_downloaded"`
	DownloadedSize int    `json:"downloaded_size"`
	Uploaded       int    `json:"blobs_uploaded"`
	UploadedSize   int    `json:"uploaded_size"`
	Duration       string `json:"sync_duration"`
	AlreadySynced  bool   `json:"already_in_sync"`
	OneWay         bool   `json:"one_way_sync"`
}

// Get fetch the given blob from the remote BlobStash instance.
func (stc *SyncClient) remotePutBlob(hash string, blob []byte) error {
	resp, err := stc.client.Post(fmt.Sprintf("/api/blobstore/blob/%s", hash), blob)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if err := clientutil.ExpectStatusCode(resp, http.StatusCreated); err != nil {
		return err
	}
	return nil
}

// Get fetch the given blob from the remote BlobStash instance.
func (stc *SyncClient) remoteGetBlob(hash string) ([]byte, error) {
	resp, err := stc.client.Get(fmt.Sprintf("/api/blobstore/blob/%s", hash))
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if err := clientutil.ExpectStatusCode(resp, http.StatusOK); err != nil {
		if err.IsNotFound() {
			return nil, clientutil.ErrBlobNotFound
		}
		return nil, err
	}

	return clientutil.Decode(resp)
}

func (stc *SyncClient) putBlob(hash string, data []byte) error {
	blob := &blob.Blob{Hash: hash, Data: data}
	if err := blob.Check(); err != nil {
		return err
	}
	return stc.blobstore.Put(context.Background(), blob)
}

func (stc *SyncClient) getBlob(hash string) ([]byte, error) {
	return stc.blobstore.Get(context.Background(), hash)
}

func (stc *SyncClient) Send(h string) error {
	blob, err := stc.getBlob(h)
	if err != nil {
		return err
	}

	if err := stc.remotePutBlob(h, blob); err != nil {
		return err
	}
	return nil
}
func (stc *SyncClient) Receive(h string) error {
	blob, err := stc.remoteGetBlob(h)
	if err != nil {
		return err
	}

	if err := stc.putBlob(h, blob); err != nil {
		return err
	}
	return nil
}

func (stc *SyncClient) Sync() (*SyncStats, error) {
	start := time.Now()
	stats := &SyncStats{
		OneWay: stc.oneWay,
	}

	local_state := stc.state.State()
	stc.state.Close()

	remote_state, err := stc.RemoteState()
	if err != nil {
		return nil, err
	}

	if local_state.Root == remote_state.Root {
		stats.Duration = time.Since(start).String()
		stats.AlreadySynced = true
		return stats, nil
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

	var upHashes, dlHashes []string

	for _, leaf := range leavesNeeded {
		// Only present on remote-side, fetch the list of hashes
		ls, err := stc.RemoteLeaf(leaf)
		if err != nil {
			return nil, err
		}
		for _, h := range ls.Hashes {
			dlHashes = append(dlHashes, h)
		}
	}

	for _, leaf := range leavesToSend {
		ls, err := stc.st.LeafState(leaf)
		if err != nil {
			return nil, err
		}
		for _, h := range ls.Hashes {
			upHashes = append(upHashes, h)
		}

	}
	for _, leaf := range leavesConflict {
		// Fetch the local leaf state
		localLeaf, err := stc.st.LeafState(leaf)
		if err != nil {
			return nil, err
		}

		// Fetch the remote leaf state
		remoteLeaf, err := stc.RemoteLeaf(leaf)
		if err != nil {
			return nil, err
		}

		// Convert the slice to map for comparison
		localIndex := slice2map(localLeaf.Hashes)
		remoteIndex := slice2map(remoteLeaf.Hashes)

		// Looks for needed blob (only present in the remote index)
		for lh, _ := range localIndex {
			if _, ok := remoteIndex[lh]; !ok {
				upHashes = append(upHashes, lh)
			}
		}
		// Find out hashes that are only present in the remote index (missing in the local index)
		for rh, _ := range remoteIndex {
			if _, ok := localIndex[rh]; !ok {
				dlHashes = append(dlHashes, rh)
			}
		}
	}

	if stc.oneWay && len(upHashes) > 0 {
		return nil, fmt.Errorf("one way sync error: found %d blobs only present locally", len(upHashes))
	}

	// Upload blobs to the remote BlobStash instances
	for _, h := range upHashes {
		blob, err := stc.getBlob(h)
		if err != nil {
			return nil, err
		}

		stats.Downloaded++
		stats.DownloadedSize += len(blob)

		if err := stc.remotePutBlob(h, blob); err != nil {
			return nil, err
		}
	}

	// Pull missing blobs from remote BlobStash instances
	for _, h := range dlHashes {
		blob, err := stc.remoteGetBlob(h)
		if err != nil {
			return nil, err
		}

		stats.Uploaded++
		stats.UploadedSize += len(blob)

		if err := stc.putBlob(h, blob); err != nil {
			return nil, err
		}
	}

	stats.Duration = time.Since(start).String()
	return stats, nil
}

func slice2map(items []string) map[string]struct{} {
	res := map[string]struct{}{}
	for _, item := range items {
		res[item] = struct{}{}
	}
	return res
}
