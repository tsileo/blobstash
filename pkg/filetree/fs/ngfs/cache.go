package main

import (
	"bytes"
	"fmt"
	"strings"
	"sync"

	"a4.io/blobstash/pkg/backend/s3/s3util"
	"a4.io/blobstash/pkg/blob"
	bcache "a4.io/blobstash/pkg/cache"
	"a4.io/blobstash/pkg/client/blobstore"
	"a4.io/blobstash/pkg/hashutil"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"golang.org/x/net/context"
)

// cache implements the blobStore interface with a local disk-backed LRU cache
type cache struct {
	fs         *FS
	bs         *blobstore.BlobStore
	mu         sync.Mutex
	remoteRefs map[string]string

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
		remoteRefs: map[string]string{},
	}, nil
}

func (c *cache) RemoteRefs() map[string]string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.remoteRefs
}

// Close implements the io.Closer interface
func (c *cache) Close() error {
	return c.blobsCache.Close()
}

// Stat implements the blobStore interface
func (c *cache) Stat(ctx context.Context, hash string) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if the blob has already been uploaded to the remote storage
	if _, ok := c.remoteRefs[hash]; ok {
		return true, nil
	}

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

// Get implements the BlobStore interface for filereader.File
func (c *cache) PutRemote(ctx context.Context, hash string, data []byte) error {
	if !c.fs.useRemote {
		return c.Put(ctx, hash, data)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	var err error

	if _, ok := c.remoteRefs[hash]; ok {
		return nil
	}

	if err := c.blobsCache.Add(hash, data); err != nil {
		return err
	}

	// Encrypt
	data, err = s3util.Seal(c.fs.key, &blob.Blob{Hash: hash, Data: data})
	if err != nil {
		return err
	}
	// Re-compute the hash
	ehash := hashutil.Compute(data)

	// Prepare the upload request
	params := &s3.PutObjectInput{
		Bucket:   aws.String(c.fs.profile.RemoteConfig.Bucket),
		Key:      aws.String("tmp/" + ehash),
		Body:     bytes.NewReader(data),
		Metadata: map[string]*string{},
	}

	// Actually upload the blob
	if _, err := c.fs.s3.PutObject(params); err != nil {
		return err
	}

	c.remoteRefs[hash] = "tmp/" + ehash

	return nil
}

// Get implements the BlobStore interface for filereader.File
func (c *cache) GetRemote(ctx context.Context, hash string) ([]byte, error) {
	if !c.fs.useRemote {
		return c.Get(ctx, hash)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	var err error
	cachedBlob, ok, err := c.blobsCache.Get(hash)
	if err != nil {
		return nil, fmt.Errorf("cache failed: %v", err)
	}
	logger.Printf("downloading %s from remote", hash)
	var data []byte
	if ok {
		data = cachedBlob
	} else {
		obj, err := s3util.NewBucket(c.fs.s3, c.fs.profile.RemoteConfig.Bucket).GetObject(hash)
		if err != nil {
			return nil, err
		}
		eblob := s3util.NewEncryptedBlob(obj, c.fs.key)
		data, err = eblob.PlainText()
		if err != nil {
			return nil, err
		}
		if err := c.blobsCache.Add(hash, data); err != nil {
			return nil, fmt.Errorf("failed to add to cache: %v", err)
		}
	}

	return data, nil
}

// Get implements the blobStore interface for filereader.File
func (c *cache) Get(ctx context.Context, hash string) ([]byte, error) {
	logger.Printf("Cache.Get(%q)\n", hash)
	if strings.HasPrefix(hash, "remote://") {
		return c.GetRemote(ctx, hash[9:])
	}

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
