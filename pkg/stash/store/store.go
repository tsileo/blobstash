package store // import "a4.io/blobstash/pkg/stash/store"

import (
	"context"

	"a4.io/blobsfile"
	"a4.io/blobstash/pkg/blob"
	"a4.io/blobstash/pkg/vkv"
)

type KvStore interface {
	Put(ctx context.Context, key, ref string, data []byte, version int) (*vkv.KeyValue, error)
	Get(ctx context.Context, key string, version int) (*vkv.KeyValue, error)
	Versions(ctx context.Context, key string, start, limit int) (*vkv.KeyValueVersions, int, error)
	Keys(ctx context.Context, start, end string, limit int) ([]*vkv.KeyValue, string, error)
	ReverseKeys(start, end string, limit int) ([]*vkv.KeyValue, string, error)
}

type BlobStore interface {
	Put(ctx context.Context, blob *blob.Blob) error
	Get(ctx context.Context, hash string) ([]byte, error)
	Stat(ctx context.Context, hash string) (bool, error)
	Enumerate(ctx context.Context, start, end string, limit int) ([]*blob.SizedBlobRef, error)
	Scan(ctx context.Context) error
}

type BlobStoreProxy struct {
	BlobStore
	ReadSrc BlobStore
}

func (p *BlobStoreProxy) Get(ctx context.Context, hash string) ([]byte, error) {
	data, err := p.BlobStore.Get(ctx, hash)
	switch err {
	case nil:
	case blobsfile.ErrBlobNotFound:
		return p.ReadSrc.Get(ctx, hash)
	default:
		return nil, err
	}
	return data, nil
}

func (p *BlobStoreProxy) Stat(ctx context.Context, hash string) (bool, error) {
	exists, err := p.BlobStore.Stat(ctx, hash)
	if err != nil {
		return false, err
	}
	if !exists {
		return p.ReadSrc.Stat(ctx, hash)
	}
	return exists, nil
}

func (p *BlobStoreProxy) Put(ctx context.Context, blob *blob.Blob) error {
	existsSrc, err := p.ReadSrc.Stat(ctx, blob.Hash)
	if err != nil {
		return err
	}
	if existsSrc {
		return nil
	}
	return p.BlobStore.Put(ctx, blob)
}
