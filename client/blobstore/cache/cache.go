package cache

import (
	"path/filepath"

	"github.com/tsileo/blobstash/backend/blobsfile"
	"github.com/tsileo/blobstash/client/blobstore"
	"github.com/tsileo/blobstash/client/clientutil"
	"github.com/tsileo/blobstash/config/pathutil"
)

// TODO(tsileo): add Clean/Reset/Remove methods

type Cache struct {
	backend *blobsfile.BlobsFileBackend
	bs      *blobstore.BlobStore
	// TODO(tsileo): embed a kvstore too (but witouth sync/), may be make it optional?
}

func New(opts *clientutil.Opts, name string) *Cache {
	return &Cache{
		bs:      blobstore.New(opts),
		backend: blobsfile.New(filepath.Join(pathutil.VarDir(), name), 0, false, false),
	}
}

func (c *Cache) Put(hash string, blob []byte) error {
	return c.backend.Put(hash, blob)
}

func (c *Cache) Stat(hash string) (bool, error) {
	exists, err := c.backend.Stat(hash)
	if err != nil {
		return false, err
	}
	if !exists {
		return c.bs.Stat(hash)
	}
	return exists, err
}

func (c *Cache) Get(hash string) ([]byte, error) {
	blob, err := c.backend.Get(hash)
	switch err {
	// If the blob is not found locally, try to fetch it from the remote blobstore
	case clientutil.ErrBlobNotFound:
		blob, err = c.bs.Get(hash)
		if err != nil {
			return nil, err
		}
		// Save the blob locally for future fetch
		if err := c.backend.Put(hash, blob); err != nil {
			return nil, err
		}
	case nil:
	default:
		return nil, err
	}
	return blob, nil
}

func (c *Cache) Sync(syncfunc func()) error {
	// TODO(tsileo): a way to sync a subtree to the remote blobstore `bs`
	// Passing a func may not be the optimal way, better to expose an Enumerate? maybe not even needed?
	return nil
}
