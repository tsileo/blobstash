package embed

import (
	"errors"
	"sync"

	"github.com/tsileo/blobstash/client/response"
	"github.com/tsileo/blobstash/router"
	"github.com/tsileo/blobstash/vkv"
)

// ErrBlobNotFound is returned from a get/stat request
// if the blob does not exist.
var ErrBlobNotFound = errors.New("blob not found")
var ErrKeyNotFound = errors.New("key doest not exist")

type KvStore struct {
	db         *vkv.DB
	kvUpdate   chan *vkv.KeyValue
	blobrouter *router.Router
}

func NewKvStore(db *vkv.DB, kvUpdate chan *vkv.KeyValue, blobrouter *router.Router) *KvStore {
	return &KvStore{
		db:         db,
		kvUpdate:   kvUpdate,
		blobrouter: blobrouter,
	}
}

func (kvs *KvStore) Put(key, value string, version int, ns string) (*response.KeyValue, error) {
	res, err := kvs.db.Put(key, value, version)
	if err != nil {
		return nil, err
	}
	res.SetNamespace(ns)
	kvs.kvUpdate <- res
	return &response.KeyValue{
		Key:     res.Key,
		Value:   res.Value,
		Version: res.Version,
	}, nil
}

func (kvs *KvStore) Get(key string, version int) (*response.KeyValue, error) {
	res, err := kvs.db.Get(key, version)
	if err != nil {
		return nil, err
	}
	return &response.KeyValue{
		Key:     res.Key,
		Value:   res.Value,
		Version: res.Version,
	}, nil
}

func (kvs *KvStore) Versions(key string, start, end, limit int) (*response.KeyValueVersions, error) {
	res, err := kvs.db.Versions(key, start, end, limit)
	if err != nil {
		return nil, err
	}
	versions := []*response.KeyValue{}
	for _, v := range res.Versions {
		versions = append(versions, &response.KeyValue{
			Key:     v.Key,
			Value:   v.Value,
			Version: v.Version,
		})
	}
	return &response.KeyValueVersions{
		Key:      res.Key,
		Versions: versions,
	}, nil
}

func (kvs *KvStore) Keys(start, end string, limit int) ([]*response.KeyValue, error) {
	res, err := kvs.db.Keys(start, end, limit)
	if err != nil {
		return nil, err
	}
	out := []*response.KeyValue{}
	for _, kv := range res {
		out = append(out, &response.KeyValue{
			Key:     kv.Key,
			Value:   kv.Value,
			Version: kv.Version,
		})
	}
	return out, nil
}

type Blob struct {
	Hash string
	Blob string
}

type BlobStore struct {
	wg         sync.WaitGroup
	stop       chan struct{}
	rblobs     chan<- *router.Blob
	blobrouter *router.Router
}

func NewBlobStore(rblobs chan<- *router.Blob, blobrouter *router.Router) *BlobStore {
	return &BlobStore{
		stop:       make(chan struct{}),
		rblobs:     rblobs,
		blobrouter: blobrouter,
	}
}
func (bs *BlobStore) ProcessBlobs() {
	return
}
func (bs *BlobStore) WaitBlobs() {
	return
}

// Get fetch the given blob.
func (bs *BlobStore) Get(hash string) ([]byte, error) {
	req := &router.Request{
		Type: router.Read,
		//	Namespace: r.URL.Query().Get("ns"),
	}
	backend := bs.blobrouter.Route(req)
	return backend.Get(hash)
}

type BlobsResp struct {
	Blobs []string `json:"blobs"`
}

func (bs *BlobStore) Enumerate(blobs chan<- string, start, end string, limit int) error {
	req := &router.Request{
		Type: router.Read,
		//	Namespace: r.URL.Query().Get("ns"),
	}
	errc := make(chan error, 0)
	backend := bs.blobrouter.Route(req)
	rblobs := make(chan string)
	go func() {
		errc <- backend.Enumerate(rblobs)
	}()
	for blob := range rblobs {
		blobs <- blob
	}
	if err := <-errc; err != nil {
		return err
	}
	return nil
}

// Stat checks wether a blob exists or not.
func (bs *BlobStore) Stat(hash string) (bool, error) {
	req := &router.Request{
		Type: router.Read,
		//	Namespace: r.URL.Query().Get("ns"),
	}
	backend := bs.blobrouter.Route(req)
	return backend.Exists(hash)
}

func (bs *BlobStore) Put(hash string, blob []byte, ns string) error {
	req := &router.Request{
		Type:      router.Write,
		Namespace: ns,
	}
	bs.rblobs <- &router.Blob{Hash: hash, Req: req, Blob: blob}
	return nil
}
