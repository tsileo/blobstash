package kvstore // import "a4.io/blobstash/pkg/kvstore"

import (
	"context"
	"fmt"
	log "github.com/inconshreveable/log15"
	"path/filepath"
	"time"

	"a4.io/blobstash/pkg/meta"
	"a4.io/blobstash/pkg/stash/store"
	"a4.io/blobstash/pkg/vkv"
)

const KvType = "kv"

// FIXME(tsileo): take a ctx as first arg for each method

type KvStore struct {
	blobStore store.BlobStore
	meta      *meta.Meta
	log       log.Logger

	vkv *vkv.DB
}

func New(logger log.Logger, dir string, blobStore store.BlobStore, metaHandler *meta.Meta) (*KvStore, error) {
	logger.Debug("init")
	kv, err := vkv.New(filepath.Join(dir, "vkv"))
	if err != nil {
		return nil, err
	}
	kvStore := &KvStore{
		blobStore: blobStore,
		meta:      metaHandler,
		log:       logger,
		vkv:       kv,
	}
	metaHandler.RegisterApplyFunc(KvType, kvStore.applyMetaFunc)
	return kvStore, nil
}

func (kv *KvStore) GetMetaBlob(ctx context.Context, key string, version int) (string, error) {
	return kv.vkv.GetMetaBlob(key, version)
}

func (kv *KvStore) applyMetaFunc(hash string, data []byte) error {
	kv.log.Debug("Apply meta init", "hash", hash)
	// applied, err := kv.vkv.MetaBlobApplied(hash)
	// if err != nil {
	// return err
	// }
	// if !applied {
	// kv.log.Debug("meta not yet applied")
	rkv, err := vkv.UnserializeBlob(data)
	if err != nil {
		return fmt.Errorf("failed to unserialize blob: %v", err)
	}
	metaBlobHash, err := kv.vkv.GetMetaBlob(rkv.Key, rkv.Version)
	if err != nil {
		return err
	}
	if metaBlobHash != "" {
		kv.log.Debug("kv already applied")
		return nil
	}

	if _, err := kv.Put(context.Background(), rkv.Key, rkv.HexHash(), rkv.Data, rkv.Version); err != nil {
		return fmt.Errorf("failed to put: %v", err)
	}
	kv.log.Debug("Applied meta", "kv", rkv)
	// }
	return nil
}

func (kv *KvStore) Close() error {
	return kv.vkv.Close()
}

func (kv *KvStore) Get(ctx context.Context, key string, version int) (*vkv.KeyValue, error) {
	kv.log.Info("OP Get", "key", key, "version", version)
	return kv.vkv.Get(key, version)
}

func (kv *KvStore) Keys(ctx context.Context, start, end string, limit int) ([]*vkv.KeyValue, string, error) {
	kv.log.Info("OP Keys", "start", "", "end", end)
	return kv.vkv.Keys(start, end, limit)
}

func (kv *KvStore) Versions(ctx context.Context, key string, start, limit int) (*vkv.KeyValueVersions, int, error) {
	kv.log.Info("OP Versions", "key", key, "start", start)
	// FIXME(tsileo): decide between -1/0 for default, or introduce a constant Max/Min?? and the end only make sense for the reverse Versions?
	if start <= 0 {
		start = int(time.Now().UTC().UnixNano())
	}
	res, cursor, err := kv.vkv.Versions(key, 0, start, limit)
	if err != nil {
		return nil, cursor, err
	}

	return res, cursor, nil
}

func (kv *KvStore) ReverseKeys(ctx context.Context, start, end string, limit int) ([]*vkv.KeyValue, string, error) {
	return kv.vkv.ReverseKeys(start, end, limit)
}

func (kv *KvStore) Put(ctx context.Context, key, ref string, data []byte, version int) (*vkv.KeyValue, error) {
	// _, fromHttp := ctxutil.Request(ctx)
	// kv.log.Info("OP Put", "from_http", fromHttp, "key", key, "value", value, "version", version)
	res := &vkv.KeyValue{
		Key:     key,
		Version: version,
		Data:    data,
	}
	if ref != "" {
		res.SetHexHash(ref)
	}
	if err := kv.vkv.Put(res); err != nil {
		return nil, err
	}

	metaBlob, err := kv.meta.Build(res)
	if err != nil {
		return nil, err
	}

	if err := kv.vkv.SetMetaBlob(key, res.Version, metaBlob.Hash); err != nil {
		return nil, err
	}

	// XXX(tsileo): notify the blobstore it does not need to exec the meta hook for this one?
	if err := kv.blobStore.Put(ctx, metaBlob); err != nil {
		return nil, err
	}

	return res, nil
}
