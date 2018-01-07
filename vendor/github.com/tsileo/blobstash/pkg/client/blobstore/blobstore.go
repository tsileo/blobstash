package blobstore // import "a4.io/blobstash/pkg/client/blobstore"

import (
	"context"
	"fmt"
	"net/http"

	"a4.io/blobstash/pkg/client/clientutil"
)

type BlobStore struct {
	client *clientutil.ClientUtil
}

func New(c *clientutil.ClientUtil) *BlobStore {
	return &BlobStore{c}
}

// Get fetch the given blob from the remote BlobStash instance.
func (bs *BlobStore) Get(ctx context.Context, hash string) ([]byte, error) {
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
func (bs *BlobStore) Stat(ctx context.Context, hash string) (bool, error) {
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

func (bs *BlobStore) Put(ctx context.Context, hash string, blob []byte) error {
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

// TODO(tsileo): add Enumerate and all other methods from the other client
