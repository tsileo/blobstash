package index // import "a4.io/blobstash/pkg/backend/s3/index"

import (
	"encoding/hex"
	"os"
	"sync"

	"github.com/cznic/kv"
)

// Queue is a FIFO queue,
type Index struct {
	db   *kv.DB
	path string
	sync.Mutex
}

// New creates a new database.
func New(path string) (*Index, error) {
	createOpen := kv.Open
	if _, err := os.Stat(path); os.IsNotExist(err) {
		createOpen = kv.Create
	}

	kvdb, err := createOpen(path, &kv.Options{})
	if err != nil {
		return nil, err
	}

	return &Index{
		db:   kvdb,
		path: path,
	}, nil
}

// Remove the underlying db file.
func (i *Index) Remove() error {
	return os.Remove(i.path)
}

// Close the underlying db file.
func (i *Index) Close() error {
	return i.db.Close()
}

func (i *Index) Index(plainHash, encryptedHash string) error {
	i.Lock()
	defer i.Unlock()
	phash, err := hex.DecodeString(plainHash)
	if err != nil {
		return err
	}
	ehash, err := hex.DecodeString(encryptedHash)
	if err != nil {
		return err
	}
	return i.db.Set(phash, ehash)
}

func (i *Index) Exists(hash string) (bool, error) {
	i.Lock()
	defer i.Unlock()
	bhash, err := hex.DecodeString(hash)
	if err != nil {
		return false, err
	}
	v, err := i.db.Get(nil, bhash)
	if err != nil {
		return false, err
	}
	if v != nil {
		return true, nil
	}
	return false, nil
}

func (i *Index) Get(hash string) (string, error) {
	i.Lock()
	defer i.Unlock()
	bhash, err := hex.DecodeString(hash)
	if err != nil {
		return "", err
	}
	v, err := i.db.Get(nil, bhash)
	if err != nil {
		return "", err
	}
	if v != nil {
		return hex.EncodeToString(v), nil
	}
	return "", nil
}
