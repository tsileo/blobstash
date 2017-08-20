package store // import "a4.io/blobstash/pkg/stash/store"

import (
	"context"
	"sort"
	"strings"

	"a4.io/blobsfile"
	"a4.io/blobstash/pkg/blob"
	"a4.io/blobstash/pkg/blobstore"
	"a4.io/blobstash/pkg/vkv"
)

type KvStore interface {
	Put(ctx context.Context, key, ref string, data []byte, version int) (*vkv.KeyValue, error)
	Get(ctx context.Context, key string, version int) (*vkv.KeyValue, error)
	Versions(ctx context.Context, key string, start, limit int) (*vkv.KeyValueVersions, int, error)
	Keys(ctx context.Context, start, end string, limit int) ([]*vkv.KeyValue, string, error)
	ReverseKeys(ctx context.Context, start, end string, limit int) ([]*vkv.KeyValue, string, error)
	Close() error
}

type KvStoreProxy struct {
	KvStore
	ReadSrc KvStore
}

func (p *KvStoreProxy) Put(ctx context.Context, key, ref string, data []byte, version int) (*vkv.KeyValue, error) {
	kv, err := p.ReadSrc.Get(ctx, key, version)
	switch err {
	case vkv.ErrNotFound:
		return p.KvStore.Put(ctx, key, ref, data, version)
	case nil:
		return kv, nil
	default:
		return nil, err

	}

	return p.KvStore.Put(ctx, key, ref, data, version)
}

func (p *KvStoreProxy) Get(ctx context.Context, key string, version int) (*vkv.KeyValue, error) {
	kv, err := p.KvStore.Get(ctx, key, version)
	switch err {
	case nil:
	case vkv.ErrNotFound:
		return p.ReadSrc.Get(ctx, key, version)
	}

	return kv, nil
}

func (p *KvStoreProxy) Versions(ctx context.Context, key string, start, limit int) (*vkv.KeyValueVersions, int, error) {
	// FIXME(tsileo): merge the output of local and root
	// versions, cursor, err :=
	return p.ReadSrc.Versions(ctx, key, start, limit)
}

func (p *KvStoreProxy) Keys(ctx context.Context, start, end string, limit int) ([]*vkv.KeyValue, string, error) {
	var cursor string
	kvs, rootCursor, err := p.ReadSrc.Keys(ctx, start, end, limit)
	if err != nil {
		return nil, cursor, err
	}

	localKvs, _, err := p.KvStore.Keys(ctx, start, end, 0)
	if err != nil {
		return nil, cursor, err
	}

	for _, lkv := range localKvs {
		if strings.Compare(lkv.Key, rootCursor) < 0 {
			kvs = append(kvs, lkv)
		}
	}

	sort.Slice(kvs, func(i, j int) bool { return kvs[i].Key > kvs[j].Key })

	if limit > 0 && len(kvs) > limit {
		kvs = kvs[0:limit]
	}

	cursor = vkv.NextKey(kvs[len(kvs)-1].Key)

	return kvs, cursor, nil
}

func (p *KvStoreProxy) ReverseKeys(ctx context.Context, start, end string, limit int) ([]*vkv.KeyValue, string, error) {
	var cursor string
	kvs, rootCursor, err := p.ReadSrc.ReverseKeys(ctx, start, end, limit)
	if err != nil {
		return nil, cursor, err
	}

	localKvs, _, err := p.KvStore.ReverseKeys(ctx, start, end, 0)
	if err != nil {
		return nil, cursor, err
	}

	for _, lkv := range localKvs {
		if strings.Compare(lkv.Key, rootCursor) > 0 {
			kvs = append(kvs, lkv)
		}
	}

	sort.Slice(kvs, func(i, j int) bool { return kvs[i].Key < kvs[j].Key })

	if limit > 0 && len(kvs) > limit {
		kvs = kvs[0:limit]
	}

	cursor = vkv.PrevKey(kvs[len(kvs)-1].Key)

	return kvs, cursor, nil
}

type BlobStore interface {
	Put(ctx context.Context, blob *blob.Blob) error
	Get(ctx context.Context, hash string) ([]byte, error)
	Stat(ctx context.Context, hash string) (bool, error)
	Enumerate(ctx context.Context, start, end string, limit int) ([]*blob.SizedBlobRef, string, error)
	Close() error
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

func (p *BlobStoreProxy) Enumerate(ctx context.Context, start, end string, limit int) ([]*blob.SizedBlobRef, string, error) {
	var out []*blob.SizedBlobRef
	var cursor string

	rootBlobs, rootCursor, err := p.ReadSrc.Enumerate(ctx, start, end, limit)
	if err != nil {
		return nil, cursor, err
	}

	out = append(out, rootBlobs...)

	localBlobs, _, err := p.BlobStore.Enumerate(ctx, start, end, 0)
	if err != nil {
		return nil, cursor, err
	}

	for _, lblob := range localBlobs {
		if strings.Compare(lblob.Hash, rootCursor) < 0 {
			out = append(out, lblob)
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Hash > out[j].Hash })

	if limit > 0 && len(out) > limit {
		out = out[0:limit]
	}

	cursor = blobstore.NextHexKey(out[len(out)-1].Hash)

	return out, cursor, nil
}
