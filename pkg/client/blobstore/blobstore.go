package blobstore // import "a4.io/blobstash/pkg/client/blobstore"

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"mime/multipart"

	//"golang.org/x/net/context"

	"a4.io/blobstash/pkg/client/clientutil"
)

var defaultServerAddr = "http://localhost:8050"
var defaultUserAgent = "BlobStore Go client v1"

type BlobStore struct {
	client *clientutil.Client
}

func DefaultOpts() *clientutil.Opts {
	return &clientutil.Opts{
		Host:              defaultServerAddr,
		UserAgent:         defaultUserAgent,
		APIKey:            "",
		EnableHTTP2:       true,
		SnappyCompression: true,
	}
}

func New(opts *clientutil.Opts) *BlobStore {
	if opts == nil {
		opts = DefaultOpts()
	}
	return &BlobStore{
		client: clientutil.New(opts),
	}
}

func (bs *BlobStore) Client() *clientutil.Client {
	return bs.client
}

// Get fetch the given blob from the remote BlobStash instance.
func (bs *BlobStore) Get(hash string) ([]byte, error) {
	resp, err := bs.client.DoReq("GET", fmt.Sprintf("/api/blobstore/blob/%s", hash), nil, nil)
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
		return nil, clientutil.ErrBlobNotFound
	default:
		return nil, fmt.Errorf("failed to get blob %v: %v", hash, string(body))
	}
}

// Stat check if the blob exists
func (bs *BlobStore) Stat(hash string) (bool, error) {
	resp, err := bs.client.DoReq("HEAD", fmt.Sprintf("/api/blobstore/blob/%s", hash), nil, nil)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return false, err
	}
	switch {
	case resp.StatusCode == 204:
		return true, nil
	case resp.StatusCode == 404:
		return false, nil
	default:
		return false, fmt.Errorf("failed to get blob %v status %d: %v", hash, resp.StatusCode, string(body))
	}
}

func (bs *BlobStore) Put(hash string, blob []byte) error {
	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)
	part, err := writer.CreateFormFile(hash, hash)
	if err != nil {
		return err
	}
	part.Write(blob)
	writer.Close()
	headers := map[string]string{"Content-Type": writer.FormDataContentType()}
	resp, err := bs.client.DoReq("POST", "/api/blobstore/upload", headers, &buf)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	switch resp.StatusCode {
	case 200, 204:
		return nil
	default:
		return fmt.Errorf("failed to put blob %v: %v", hash, string(body))
	}
}

// TODO(tsileo): add Enumerate and all other methods from the other client
