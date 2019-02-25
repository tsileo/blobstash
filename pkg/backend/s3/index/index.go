package index // import "a4.io/blobstash/pkg/backend/s3/index"

import (
	"encoding/hex"
	"sync"

	"github.com/recoilme/pudge"
)

// Queue is a FIFO queue,
type Index struct {
	db   *pudge.Db
	path string
	sync.Mutex
}

// New creates a new database.
func New(path string) (*Index, error) {
	kvdb, err := pudge.Open(path, nil)
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
	return i.db.DeleteFile()
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
	exists, err := i.db.Has(bhash)
	if err != nil {
		return false, err
	}
	return exists, nil
}

func (i *Index) Get(hash string) (string, error) {
	i.Lock()
	defer i.Unlock()
	bhash, err := hex.DecodeString(hash)
	if err != nil {
		return "", err
	}
	var v []byte
	if err := i.db.Get(bhash, &v); err != nil {
		return "", err
	}
	if v != nil {
		return hex.EncodeToString(v), nil
	}
	return "", nil
}
