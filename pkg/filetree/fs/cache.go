package fs

import (
	"fmt"
	"sync"

	bcache "a4.io/blobstash/pkg/cache"
	"a4.io/blobstash/pkg/client/blobstore"
	"golang.org/x/net/context"
)

// cache implements the blobStore interface with a local disk-backed LRU cache
type cache struct {
	fs *FS
	bs *blobstore.BlobStore
	mu sync.Mutex

	blobsCache *bcache.Cache
}

// newCache initializes a new cache instance
func newCache(fs *FS, bs *blobstore.BlobStore, path string) (*cache, error) {
	blobsCache, err := bcache.New(path, "blobs.cache", (5*1024)<<20) // 5GB on-disk LRU cache TODO(tsileo): make it configurable
	if err != nil {
		return nil, err
	}

	return &cache{
		fs:         fs,
		bs:         bs,
		blobsCache: blobsCache,
	}, nil
}

// Close implements the io.Closer interface
func (c *cache) Close() error {
	return c.blobsCache.Close()
}

// Stat implements the blobStore interface
func (c *cache) Stat(ctx context.Context, hash string) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	stat, err := c.bs.Stat(context.TODO(), hash)
	if err != nil {
		return false, err
	}

	return stat, nil
}

// Put implements the blobStore interface for filereader.File
func (c *cache) Put(ctx context.Context, hash string, data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.bs.Put(ctx, hash, data); err != nil {
		return err
	}

	if err := c.blobsCache.Add(hash, data); err != nil {
		return err
	}
	return nil
}

// Get implements the blobStore interface for filereader.File
func (c *cache) Get(ctx context.Context, hash string) ([]byte, error) {
	logger.Printf("Cache.Get(%q)\n", hash)
	var err error
	cachedBlob, ok, err := c.blobsCache.Get(hash)
	if err != nil {
		return nil, fmt.Errorf("cache failed: %v", err)
	}
	var data []byte
	if ok {
		data = cachedBlob
	} else {
		c.mu.Lock()
		defer c.mu.Unlock()

		data, err = c.bs.Get(ctx, hash)
		if err != nil {
			return nil, fmt.Errorf("failed to call blobstore: %v", err)
		}
		if err := c.blobsCache.Add(hash, data); err != nil {
			return nil, fmt.Errorf("failed to add to cache: %v", err)
		}
	}
	return data, nil
}
