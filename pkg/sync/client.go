package sync

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"mime/multipart"
	"time"

	"a4.io/blobstash/pkg/blob"
	"a4.io/blobstash/pkg/blobstore"
	"a4.io/blobstash/pkg/client/clientutil"

	log "github.com/inconshreveable/log15"
)

type SyncTableClient struct {
	client *clientutil.Client

	url    string
	apiKey string

	blobstore *blobstore.BlobStore

	st    *SyncTable
	state *StateTree

	log log.Logger
}

func NewSyncTableClient(logger log.Logger, st *SyncTable, state *StateTree, blobstore *blobstore.BlobStore, url, apiKey string) *SyncTableClient {
	clientOpts := &clientutil.Opts{
		APIKey:            apiKey,
		Host:              url,
		EnableHTTP2:       true,
		SnappyCompression: false, // FIXME(tsileo): Activate this once snappy response reader is imported
	}
	return &SyncTableClient{
		client:    clientutil.New(clientOpts),
		url:       url,
		apiKey:    apiKey,
		st:        st,
		state:     state,
		blobstore: blobstore,
	}
}

func (stc *SyncTableClient) RemoteState() (*State, error) {
	s := &State{}
	if err := stc.client.GetJSON("/api/sync/state", nil, s); err != nil {
		return nil, err
	}
	return s, nil
}

func (stc *SyncTableClient) RemoteLeaf(prefix string) (*LeafState, error) {
	ls := &LeafState{}
	if err := stc.client.GetJSON(fmt.Sprintf("/api/sync/state/leaf/%s", prefix), nil, ls); err != nil {
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
}

// Get fetch the given blob from the remote BlobStash instance.
func (stc *SyncTableClient) remotePutBlob(hash string, blob []byte) error {
	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)
	part, err := writer.CreateFormFile(hash, hash)
	if err != nil {
		return err
	}
	part.Write(blob)

	headers := map[string]string{"Content-Type": writer.FormDataContentType()}
	resp, err := stc.client.DoReq("POST", "/api/blobstore/upload", headers, &buf)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	switch {
	case resp.StatusCode == 200:
		return nil
	default:
		return fmt.Errorf("failed to put blob %v: %v", hash, string(body))
	}
}

// Get fetch the given blob from the remote BlobStash instance.
func (stc *SyncTableClient) remoteGetBlob(hash string) ([]byte, error) {
	resp, err := stc.client.DoReq("GET", fmt.Sprintf("/api/blobstore/blob/%s", hash), nil, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode == 200:
		sr := clientutil.NewSnappyResponseReader(resp)
		defer sr.Close()
		return ioutil.ReadAll(sr)
	case resp.StatusCode == 404:
		return nil, fmt.Errorf("Blob %s not found", hash)
	default:
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("failed to get blob %v: %v", hash, string(body))
	}
}

func (stc *SyncTableClient) putBlob(hash string, data []byte) error {
	blob := &blob.Blob{Hash: hash, Data: data}
	if err := blob.Check(); err != nil {
		return err
	}
	return stc.blobstore.Put(context.Background(), blob)
}

func (stc *SyncTableClient) getBlob(hash string) ([]byte, error) {
	return stc.blobstore.Get(context.Background(), hash)
}

func (stc *SyncTableClient) Sync() (*SyncStats, error) {
	start := time.Now()
	stats := &SyncStats{}

	local_state := stc.state.State()

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
