package blobstore // import "a4.io/blobstash/pkg/client/blobstore"

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"mime/multipart"
	"net/http"

	"a4.io/blobstash/pkg/client/clientutil"
)

// FIXME(tsileo): support data contexes

var defaultServerAddr = "http://localhost:8050"
var defaultUserAgent = "BlobStore Go client v1"

type BlobStore2 struct {
	client *clientutil.ClientUtil
}

func New2(c *clientutil.ClientUtil) *BlobStore2 {
	return &BlobStore2{c}
}

// Get fetch the given blob from the remote BlobStash instance.
func (bs *BlobStore2) Get(ctx context.Context, hash string) ([]byte, error) {
	resp, err := bs.client.Get(fmt.Sprintf("/api/blobstore/blob/%s", hash))
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

// Stat check if the blob exists
func (bs *BlobStore2) Stat(ctx context.Context, hash string) (bool, error) {
	resp, err := bs.client.Head(fmt.Sprintf("/api/blobstore/blob/%s", hash))
	if err != nil {
		return false, err
	}

	defer resp.Body.Close()

	if err := clientutil.ExpectStatusCode(resp, http.StatusNoContent); err != nil {
		if err.IsNotFound() {
			return false, nil
		}

		return false, err
	}

	return true, nil
}

func (bs *BlobStore2) Put(ctx context.Context, hash string, blob []byte) error {
	resp, err := bs.client.Post(fmt.Sprintf("/api/blobstore/blob/%s", hash), blob)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if err := clientutil.ExpectStatusCode(resp, http.StatusCreated); err != nil {
		return err
	}

	return nil
}

type BlobStore struct {
	client *clientutil.Client
}

func DefaultOpts() *clientutil.Opts {
	return &clientutil.Opts{
		Host:              defaultServerAddr,
		UserAgent:         defaultUserAgent,
		APIKey:            "",
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
func (bs *BlobStore) Get(ctx context.Context, hash string) ([]byte, error) {
	resp, err := bs.client.DoReq(ctx, "GET", fmt.Sprintf("/api/blobstore/blob/%s", hash), nil, nil)
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
func (bs *BlobStore) Stat(ctx context.Context, hash string) (bool, error) {
	resp, err := bs.client.DoReq(ctx, "HEAD", fmt.Sprintf("/api/blobstore/blob/%s", hash), nil, nil)
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

func (bs *BlobStore) Put(ctx context.Context, hash string, blob []byte) error {
	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)
	part, err := writer.CreateFormFile(hash, hash)
	if err != nil {
		return err
	}
	part.Write(blob)
	writer.Close()
	headers := map[string]string{"Content-Type": writer.FormDataContentType()}
	resp, err := bs.client.DoReq(ctx, "POST", "/api/blobstore/upload", headers, &buf)
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
