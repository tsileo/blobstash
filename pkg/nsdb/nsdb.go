package nsdb

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/cznic/kv"
	log "github.com/inconshreveable/log15"
	"golang.org/x/net/context"

	"github.com/tsileo/blobstash/pkg/blob"
	"github.com/tsileo/blobstash/pkg/blobstore"
	"github.com/tsileo/blobstash/pkg/config"
	"github.com/tsileo/blobstash/pkg/ctxutil"
	"github.com/tsileo/blobstash/pkg/hub"
	"github.com/tsileo/blobstash/pkg/meta"
)

const NsType = "ns"

type NsMeta struct {
	Hash      string `json:"hash"`
	Namespace string `json:"ns"`
}

func NewNsMeta(hash, namespace string) *NsMeta {
	return &NsMeta{
		Hash:      hash,
		Namespace: namespace,
	}
}

func (ns *NsMeta) Type() string {
	return NsType
}

func (ns *NsMeta) Dump() ([]byte, error) {
	return json.Marshal(ns)
}

type DB struct {
	db        *kv.DB
	path      string
	meta      *meta.Meta
	hub       *hub.Hub
	log       log.Logger
	conf      *config.Config
	blobStore *blobstore.BlobStore
	sync.Mutex
}

// New creates a new database.
func New(logger log.Logger, conf *config.Config, blobStore *blobstore.BlobStore, m *meta.Meta, chub *hub.Hub) (*DB, error) {
	logger.Debug("init")
	path := filepath.Join(conf.VarDir(), "nsdb")
	createOpen := kv.Open
	if _, err := os.Stat(path); os.IsNotExist(err) {
		createOpen = kv.Create
	}
	kvdb, err := createOpen(path, &kv.Options{})
	if err != nil {
		return nil, err
	}
	nsdb := &DB{
		hub:       chub,
		db:        kvdb,
		meta:      m,
		log:       logger,
		blobStore: blobStore,
		path:      path,
		conf:      conf,
	}
	m.RegisterApplyFunc(NsType, nsdb.applyMetaFunc)
	nsdb.hub.Subscribe(hub.NewBlob, "nsdb", nsdb.newBlobCallback)
	return nsdb, nil
}

func (db *DB) newBlobCallback(ctx context.Context, blob *blob.Blob, _ interface{}) error {
	metaType, _, isMeta := meta.IsMetaBlob(blob.Data)
	db.log.Debug("newBlobCallback", "is_meta", isMeta, "meta_type", metaType)
	if !(isMeta && metaType == NsType) {
		if ns, ok := ctxutil.Namespace(ctx); ok {
			db.log.Debug("creating namespace blob", "hash", blob.Hash, "namespace", ns)
			if err := db.AddNs(blob.Hash, ns); err != nil {
				return err
			}
		}
	}
	return nil
}

func (db *DB) Close() error {
	return db.db.Close()
}

func (db *DB) Destroy() error {
	if db.path != "" {
		db.Close()
		return os.RemoveAll(db.path)
	}
	return nil
}

func (db *DB) applyMetaFunc(hash string, data []byte) error {
	db.log.Debug("Applying meta init", "hash", hash)
	nsMeta := &NsMeta{}
	if err := json.Unmarshal(data, nsMeta); err != nil {
		return err
	}
	applied, err := db.blobApplied(hash)
	if err != nil {
		return err
	}
	if !applied {
		db.log.Debug("Applying ns meta", "ns", nsMeta)
		if err := db.addNs(nsMeta.Hash, nsMeta.Namespace); err != nil {
			return err
		}
		if err := db.applyMeta(hash); err != nil {
			return err
		}
	}
	return nil

}

func encodeKey(hexHash, ns string) []byte {
	hash, err := hex.DecodeString(hexHash)
	if err != nil {
		panic(err)
	}
	var buf bytes.Buffer
	buf.WriteString(ns + ":")
	buf.Write(hash)
	return buf.Bytes()
}

func (db *DB) AddNs(hexHash, ns string) error {
	if err := db.addNs(hexHash, ns); err != nil {
		return err
	}
	nsMeta := NewNsMeta(hexHash, ns)
	// Save the meta blob
	metaBlob, err := db.meta.Build(nsMeta)
	if err != nil {
		return err
	}
	// Set it as applied, so it won't be re-applied
	db.applyMeta(metaBlob.Hash)
	ctx := ctxutil.WithNamespace(context.Background(), ns)
	return db.blobStore.Put(ctx, metaBlob)
}

func (db *DB) addNs(hexHash, ns string) error {
	return db.db.Set(encodeKey(hexHash, ns), []byte{})
}

// Namespaces returns all blobs for the given namespace
func (db *DB) Namespace(ns, prefix string) ([]string, error) {
	res := []string{}
	prefixBytes, err := hex.DecodeString(prefix)
	if err != nil {
		return nil, err
	}
	start := []byte(fmt.Sprintf("%s:%s", ns, string(prefixBytes)))
	vstart := len(ns) + 1
	enum, _, err := db.db.Seek(start)
	if err != nil {
		return nil, err
	}
	endBytes := append(start, '\xff')
	for {
		k, _, err := enum.Next()
		if err == io.EOF {
			break
		}
		if bytes.Compare(k, endBytes) > 0 {
			return res, nil
		}
		res = append(res, fmt.Sprintf("%x", k[vstart:]))
	}
	return res, nil
}

func (db *DB) applyMeta(hash string) error {
	return db.db.Set([]byte(fmt.Sprintf("_meta:%s", hash)), []byte("1"))
}

func (db *DB) blobApplied(hash string) (bool, error) {
	res, err := db.db.Get(nil, []byte(fmt.Sprintf("_meta:%s", hash)))
	if err != nil {
		return false, err
	}
	if res == nil || len(res) == 0 {
		return false, nil
	}
	return true, nil
}
