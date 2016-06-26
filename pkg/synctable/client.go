package synctable

import (
	"bytes"
	"encoding/json"
	"fmt"
	"golang.org/x/net/context"
	"io/ioutil"
	"mime/multipart"
	"time"

	"github.com/tsileo/blobstash/pkg/blob"
	"github.com/tsileo/blobstash/pkg/blobstore"
	"github.com/tsileo/blobstash/pkg/client/clientutil"
	"github.com/tsileo/blobstash/pkg/ctxutil"
	"github.com/tsileo/blobstash/pkg/nsdb"

	log "github.com/inconshreveable/log15"
)

type SyncTableClient struct {
	client    *clientutil.Client
	url       string
	apiKey    string
	namespace string
	nsdb      *nsdb.DB
	blobstore *blobstore.BlobStore
	state     *State
	log       log.Logger
}

// func New(logger log.Logger, blobStore *blobstore.BlobStore, metaHandler *meta.Meta) (*KvStore, error) {
func NewSyncTableClient(logger log.Logger, state *State, blobstore *blobstore.BlobStore, nsDB *nsdb.DB, ns, url, apiKey string) *SyncTableClient {
	clientOpts := &clientutil.Opts{
		APIKey:            apiKey,
		Host:              url,
		Namespace:         ns,
		EnableHTTP2:       true,
		SnappyCompression: false, // FIXME(tsileo): Activate this once snappy response reader is imported
	}
	return &SyncTableClient{
		client:    clientutil.New(clientOpts),
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
	resp, err := stc.client.DoReq("GET", fmt.Sprintf("/api/sync/_state/%s/leafs/%s", stc.namespace, prefix), nil, nil)
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
	resp, err := stc.client.DoReq("POST", fmt.Sprintf("/api/blobstore/upload?"), headers, &buf)
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

func (stc *SyncTableClient) saveBlob(hash string, data []byte) error {
	blob := &blob.Blob{Hash: hash, Data: data}
	if err := blob.Check(); err != nil {
		return err
	}
	ctx := ctxutil.WithNamespace(context.Background(), stc.namespace)
	return stc.blobstore.Put(ctx, blob)
}

func (stc *SyncTableClient) Sync() (*SyncStats, error) {
	start := time.Now()
	stats := &SyncStats{}
	js, err := json.Marshal(stc.state)
	if err != nil {
		return nil, err
	}
	payload := bytes.NewReader(js)

	resp, err := stc.client.DoReq("POST", fmt.Sprintf("/api/sync/%s", stc.namespace), nil, payload)
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
			blob, err := stc.blobstore.Get(context.Background(), h)
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
