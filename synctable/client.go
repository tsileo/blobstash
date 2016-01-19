package synctable

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"mime/multipart"
	"time"

	"github.com/tsileo/blobstash/client/clientutil"
	"github.com/tsileo/blobstash/embed"
	"github.com/tsileo/blobstash/nsdb"
	"github.com/tsileo/blobstash/router"

	"github.com/dchest/blake2b"
)

type SyncTableClient struct {
	client    *clientutil.Client
	url       string
	apiKey    string
	namespace string
	nsdb      *nsdb.DB
	blobstore *embed.BlobStore
	blobs     chan<- *router.Blob
	state     *State
}

func NewSyncTableClient(state *State, blobstore *embed.BlobStore, nsDB *nsdb.DB, ns, url, apiKey string, blobs chan<- *router.Blob) *SyncTableClient {
	// Only enable HTTP2 if the remote url uses HTTPS
	clientOpts := &clientutil.Opts{
		APIKey:    apiKey,
		Host:      url,
		Namespace: ns,
	}
	return &SyncTableClient{
		client:    clientutil.New(clientOpts),
		blobs:     blobs,
		url:       url,
		nsdb:      nsDB,
		apiKey:    apiKey,
		state:     state,
		blobstore: blobstore,
		namespace: ns,
	}
}

func (stc *SyncTableClient) Leafs(prefix string) (*LeafState, error) {
	ls := &LeafState{}
	resp, err := stc.client.DoReq("GET", fmt.Sprintf("/api/sync/v1/_state/%s/leafs/%s", stc.namespace, prefix), nil, nil)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	switch resp.StatusCode {
	case 200:
		if err := json.NewDecoder(resp.Body).Decode(ls); err != nil {
			return nil, err
		}
		return ls, nil
	default:
		var body bytes.Buffer
		body.ReadFrom(resp.Body)
		return nil, fmt.Errorf("failed to insert doc: %v", body.String())
	}
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
func (stc *SyncTableClient) PutBlob(hash string, blob []byte, ns string) error {
	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)
	part, err := writer.CreateFormFile(hash, hash)
	if err != nil {
		return err
	}
	part.Write(blob)

	headers := map[string]string{"Content-Type": writer.FormDataContentType()}
	resp, err := stc.client.DoReq("POST", fmt.Sprintf("/api/v1/blobstore/upload?ns=%s", ns), headers, &buf)
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
func (stc *SyncTableClient) GetBlob(hash string) ([]byte, error) {
	resp, err := stc.client.DoReq("GET", fmt.Sprintf("/api/v1/blobstore/blob/%s", hash), nil, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	switch {
	case resp.StatusCode == 200:
		return body, nil
	case resp.StatusCode == 404:
		return nil, fmt.Errorf("Blob %s not found", hash)
	default:
		return nil, fmt.Errorf("failed to get blob %v: %v", hash, string(body))
	}
}

func (stc *SyncTableClient) saveBlob(hash string, blob []byte) error {
	req := &router.Request{
		Type:      router.Write,
		Namespace: stc.namespace,
	}
	// Ensures the blob isn't corrupted
	chash := fmt.Sprintf("%x", blake2b.Sum256(blob))
	if hash != chash {
		return fmt.Errorf("Blob %s is corrupted", hash)
	}
	stc.blobs <- &router.Blob{Hash: hash, Req: req, Blob: blob}
	return nil
}

func (stc *SyncTableClient) Sync() (*SyncStats, error) {
	start := time.Now()
	stats := &SyncStats{}
	js, err := json.Marshal(stc.state)
	if err != nil {
		return nil, err
	}
	payload := bytes.NewReader(js)

	resp, err := stc.client.DoReq("POST", fmt.Sprintf("/api/sync/v1/%s", stc.namespace), nil, payload)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case 204:
		stats.Duration = time.Since(start).String()
		stats.AlreadySynced = true
		return stats, nil
	case 200:
		sr := &SyncResp{}
		if err := json.NewDecoder(resp.Body).Decode(sr); err != nil {
			return nil, err
		}
		uploadQueue := []string{}
		downloadQueue := []string{}
		// Blindly upload all the blobs "needed" by the remote
		for _, prefix := range sr.Needed {
			hashes, err := stc.nsdb.Namespace(stc.namespace, prefix)
			if err != nil {
				return nil, err
			}
			uploadQueue = append(uploadQueue, hashes...)
		}
		// Discover which one are missing/need to be uploaded
		for _, prefix := range sr.Conflicted {
			localHashes, err := stc.nsdb.Namespace(stc.namespace, prefix)
			if err != nil {
				return nil, err
			}
			remoteLeafs, err := stc.Leafs(prefix)
			if err != nil {
				return nil, err
			}
			localIndex := slice2map(localHashes)
			remoteIndex := slice2map(remoteLeafs.Hashes)
			// Find out hashes that are present in the local index (missing in the remote index)
			for lh, _ := range localIndex {
				if _, ok := remoteIndex[lh]; !ok {
					uploadQueue = append(uploadQueue, lh)
				}
			}
			// Find out hashes that are only present in the remote index (missing in the local index)
			for rh, _ := range remoteIndex {
				if _, ok := localIndex[rh]; !ok {
					downloadQueue = append(downloadQueue, rh)
				}
			}
		}
		// Blindly fetch all the missing blobs from the "missing" leafs
		for _, prefix := range sr.Missing {
			leafs, err := stc.Leafs(prefix)
			if err != nil {
				return nil, err
			}
			downloadQueue = append(downloadQueue, leafs.Hashes...)
		}
		// TODO(tsileo): debug logging, and consume the queue in a goroutine
		for _, h := range downloadQueue {
			// Fetch the blob from the remote instance
			blob, err := stc.GetBlob(h)
			if err != nil {
				return nil, err
			}
			// And save it in the local blob store
			if err := stc.saveBlob(h, blob); err != nil {
				return nil, err
			}
			// Update the stats
			stats.DownloadedSize += len(blob)
			stats.Downloaded++
		}
		for _, h := range uploadQueue {
			// Fetch the blob locally
			blob, err := stc.blobstore.Get(h)
			if err != nil {
				return nil, err
			}
			// Upload the blob
			if err := stc.PutBlob(h, blob, stc.namespace); err != nil {
				return nil, err
			}
			stats.Uploaded++
			stats.UploadedSize += len(blob)
		}
		stats.Duration = time.Since(start).String()
		return stats, nil
	default:
		var body bytes.Buffer
		body.ReadFrom(resp.Body)
		return nil, fmt.Errorf("failed to insert doc: %v", body.String())
	}
}

func slice2map(items []string) map[string]struct{} {
	res := map[string]struct{}{}
	for _, item := range items {
		res[item] = struct{}{}
	}
	return res
}
