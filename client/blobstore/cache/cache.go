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
	return c.backend.Stat(hash)
}

func (c *Cache) Get(hash string) ([]byte, error) {
	// TODO(tsileo): if blob doesn't exist, try to fetch it from the remote blobstore AND cache it locally
	return c.backend.Get(hash)
}

func (c *Cache) Sync(syncfunc func()) error {
	// TODO(tsileo): a way to sync a subtree to the remote blobstore `bs`
	// Passing a func may not be the optimal way, better to expose an Enumerate? maybe not even needed?
	return nil
}
