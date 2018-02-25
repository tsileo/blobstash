package store // import "a4.io/blobstash/pkg/stash/store"

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"a4.io/blobsfile"
	"a4.io/blobstash/pkg/blob"
	"a4.io/blobstash/pkg/blobstore"
	"a4.io/blobstash/pkg/vkv"
)

var sepCandidates = []string{":", "&", "*", "^", "#", ".", "-", "_", "+", "=", "%", "@", "!"}

type sortHelper struct {
	Item       interface{}
	IsFromRoot bool
}

type mergeCursor struct {
	rstart, sstart   string
	rcursor, scursor string
}

func (c *mergeCursor) Encode(nextKey func(string) string) string {
	rcursor := nextKey(c.rcursor)
	scursor := nextKey(c.scursor)

	var sep string
	// Find a separator that is not in the key
	for _, c := range sepCandidates {
		if !strings.Contains(rcursor, c) && !strings.Contains(scursor, c) {
			sep = c
			break
		}
	}
	return fmt.Sprintf("stash:%s%s%s%s", sep, rcursor, sep, scursor)
}

func parseCursor(start string) *mergeCursor {
	c := &mergeCursor{}
	// Check if the cursor is a "merge cursor", i.e. in the format:
	// "stash:<root cursor>:<stash cursor>"
	if strings.HasPrefix(start, "stash:") {
		sep := start[6:7]
		cdata := strings.Split(start[7:len(start)], sep)
		c.rstart = cdata[0]
		c.sstart = cdata[1]

		// Initialize the part of the merge cursor to the "start" cursor,
		// as it's possible the current range will return nothing either in
		// the root or the stash
		c.rcursor = c.rstart
		c.scursor = c.sstart
	} else {
		c.rstart = start
		c.sstart = start

		// Same here for the cursor
		c.rcursor = start
		c.scursor = start
	}
	return c
}

type DataContext interface {
	BlobStore() BlobStore
	KvStore() KvStore
	BlobStoreProxy() BlobStore
	KvStoreProxy() KvStore
	Merge(context.Context) error
	Close() error
	Closed() bool
	Destroy() error
}

type KvStore interface {
	Put(ctx context.Context, key, ref string, data []byte, version int64) (*vkv.KeyValue, error)
	Get(ctx context.Context, key string, version int64) (*vkv.KeyValue, error)
	GetMetaBlob(ctx context.Context, key string, version int64) (string, error)
	Versions(ctx context.Context, key, start string, limit int) (*vkv.KeyValueVersions, string, error)
	Keys(ctx context.Context, start, end string, limit int) ([]*vkv.KeyValue, string, error)
	ReverseKeys(ctx context.Context, start, end string, limit int) ([]*vkv.KeyValue, string, error)
	Close() error
}

type KvStoreProxy struct {
	KvStore
	ReadSrc KvStore
}

func (p *KvStoreProxy) Put(ctx context.Context, key, ref string, data []byte, version int64) (*vkv.KeyValue, error) {
	if version > 0 {
		kv, err := p.ReadSrc.Get(ctx, key, version)
		switch err {
		case vkv.ErrNotFound:
			return p.KvStore.Put(ctx, key, ref, data, version)
		case nil:
			return kv, nil
		default:
		}
	}

	return p.KvStore.Put(ctx, key, ref, data, version)
}

func (p *KvStoreProxy) Get(ctx context.Context, key string, version int64) (*vkv.KeyValue, error) {
	kv, err := p.KvStore.Get(ctx, key, version)
	switch err {
	case nil:
		// The "latest" version is requested, we need to compare with the "root" kv store
		// to return the latest between the two
		if version <= 0 {
			rkv, rerr := p.ReadSrc.Get(ctx, key, version)
			if rerr != nil && rerr != vkv.ErrNotFound {
				return nil, err
			}
			if rerr == nil && rkv.Version > kv.Version {
				// The one from the "root" kv store is more recent, return it
				return rkv, nil
			}
		}
	case vkv.ErrNotFound:
		return p.ReadSrc.Get(ctx, key, version)
	default:
		return nil, err
	}
	return kv, nil
}

func (p *KvStoreProxy) GetMetaBlob(ctx context.Context, key string, version int64) (string, error) {
	h, err := p.KvStore.GetMetaBlob(ctx, key, version)
	switch err {
	case nil:
	case vkv.ErrNotFound:
		return p.ReadSrc.GetMetaBlob(ctx, key, version)
	default:
		return "", err
	}
	return h, nil
}

func (p *KvStoreProxy) Versions(ctx context.Context, key, start string, limit int) (*vkv.KeyValueVersions, string, error) {
	var tmp []*sortHelper
	var out []*vkv.KeyValue
	res := &vkv.KeyValueVersions{
		Key: key,
	}

	mcursor := parseCursor(start)

	versions, _, err := p.ReadSrc.Versions(ctx, key, mcursor.rstart, limit)
	if err != nil && err != vkv.ErrNotFound {
		return nil, "", err
	}

	// The key may not exist
	if err == nil {
		for _, kv := range versions.Versions {
			tmp = append(tmp, &sortHelper{kv, true})
		}
	}

	localVersions, _, err := p.KvStore.Versions(ctx, key, mcursor.sstart, limit)
	if err != nil && err != vkv.ErrNotFound {
		return nil, "", err
	}

	// The key may not exist
	if err == nil {
		for _, kv := range localVersions.Versions {
			tmp = append(tmp, &sortHelper{kv, false})
		}
	}

	// Sort everything
	sort.Slice(tmp, func(i, j int) bool {
		return tmp[i].Item.(*vkv.KeyValue).Version > tmp[j].Item.(*vkv.KeyValue).Version
	})

	// Slice it if it's too big
	if len(tmp) > 0 && len(tmp) > limit {
		tmp = tmp[0:limit]
	}

	// Build the final result, and compute the "merge cursor"
	for _, sh := range tmp {
		kv := sh.Item.(*vkv.KeyValue)
		if sh.IsFromRoot {
			mcursor.rcursor = strconv.FormatInt(kv.Version, 10)
		} else {
			mcursor.scursor = strconv.FormatInt(kv.Version, 10)
		}
		out = append(out, kv)
	}

	res.Versions = out

	return res, mcursor.Encode(vkv.NextVersionCursor), nil
}

func (p *KvStoreProxy) ReverseKeys(ctx context.Context, start, end string, limit int) ([]*vkv.KeyValue, string, error) {
	var tmp []*sortHelper
	var out []*vkv.KeyValue

	mcursor := parseCursor(start)

	kvs, _, err := p.ReadSrc.ReverseKeys(ctx, mcursor.rstart, end, limit)
	if err != nil {
		return nil, "", err
	}

	for _, kv := range kvs {
		tmp = append(tmp, &sortHelper{kv, true})
	}

	localKvs, _, err := p.KvStore.ReverseKeys(ctx, mcursor.sstart, end, 0)
	if err != nil {
		return nil, "", err
	}

	for _, kv := range localKvs {
		tmp = append(tmp, &sortHelper{kv, false})
	}

	// Sort everything
	sort.Slice(tmp, func(i, j int) bool {
		return tmp[i].Item.(*vkv.KeyValue).Key > tmp[j].Item.(*vkv.KeyValue).Key
	})

	// Slice it if it's too big
	if len(tmp) > 0 && len(tmp) > limit {
		tmp = tmp[0:limit]
	}

	// Build the final result, and compute the "merge cursor"
	for _, sh := range tmp {
		kv := sh.Item.(*vkv.KeyValue)
		if sh.IsFromRoot {
			mcursor.rcursor = kv.Key
		} else {
			mcursor.scursor = kv.Key
		}
		out = append(out, kv)
	}

	return out, mcursor.Encode(vkv.NextKey), nil
}

func (p *KvStoreProxy) Keys(ctx context.Context, start, end string, limit int) ([]*vkv.KeyValue, string, error) {
	var tmp []*sortHelper
	var out []*vkv.KeyValue

	mcursor := parseCursor(start)

	kvs, _, err := p.ReadSrc.Keys(ctx, mcursor.rstart, end, limit)
	if err != nil {
		return nil, "", err
	}

	for _, kv := range kvs {
		tmp = append(tmp, &sortHelper{kv, true})
	}

	localKvs, _, err := p.KvStore.Keys(ctx, mcursor.sstart, end, 0)
	if err != nil {
		return nil, "", err
	}

	for _, kv := range localKvs {
		tmp = append(tmp, &sortHelper{kv, false})
	}

	if limit > 0 && len(kvs) > limit {
		kvs = kvs[0:limit]
	}

	// Sort everything
	sort.Slice(tmp, func(i, j int) bool {
		return tmp[i].Item.(*vkv.KeyValue).Key < tmp[j].Item.(*vkv.KeyValue).Key
	})

	// Slice it if it's too big
	if len(tmp) > 0 && len(tmp) > limit {
		tmp = tmp[0:limit]
	}

	// Build the final result, and compute the "merge cursor"
	for _, sh := range tmp {
		kv := sh.Item.(*vkv.KeyValue)
		if sh.IsFromRoot {
			mcursor.rcursor = kv.Key
		} else {
			mcursor.scursor = kv.Key
		}
		out = append(out, kv)
	}

	return out, mcursor.Encode(vkv.NextKey), nil
}

type BlobStore interface {
	Put(ctx context.Context, blob *blob.Blob) error
	Get(ctx context.Context, hash string) ([]byte, error)
	GetEncoded(ctx context.Context, hash string) ([]byte, error)
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

func (p *BlobStoreProxy) GetEncoded(ctx context.Context, hash string) ([]byte, error) {
	data, err := p.BlobStore.GetEncoded(ctx, hash)
	switch err {
	case nil:
	case blobsfile.ErrBlobNotFound:
		return p.ReadSrc.GetEncoded(ctx, hash)
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
	// Here, we will need to merge two differents "enumerate results" into one
	var tmp []*sortHelper
	var out []*blob.SizedBlobRef

	mcursor := parseCursor(start)

	// Fetch the data from the "root" blobstore
	rootBlobs, _, err := p.ReadSrc.Enumerate(ctx, mcursor.rstart, end, limit)
	if err != nil {
		return nil, "", err
	}

	for _, blob := range rootBlobs {
		tmp = append(tmp, &sortHelper{blob, true})
	}

	// Fetch the data from the stash
	localBlobs, _, err := p.BlobStore.Enumerate(ctx, mcursor.sstart, end, 0)
	if err != nil {
		return nil, "", err
	}

	for _, blob := range localBlobs {
		tmp = append(tmp, &sortHelper{blob, false})
	}

	// Sort everything
	sort.Slice(tmp, func(i, j int) bool {
		return tmp[i].Item.(*blob.SizedBlobRef).Hash > tmp[j].Item.(*blob.SizedBlobRef).Hash
	})

	// Slice it if it's too big
	if len(tmp) > 0 && len(tmp) > limit {
		tmp = tmp[0:limit]
	}

	// Build the final result, and compute the "merge cursor"
	for _, sh := range tmp {
		b := sh.Item.(*blob.SizedBlobRef)
		if sh.IsFromRoot {
			mcursor.rcursor = b.Hash
		} else {
			mcursor.scursor = b.Hash
		}
		out = append(out, b)
	}

	return out, mcursor.Encode(blobstore.NextHexKey), nil
}
