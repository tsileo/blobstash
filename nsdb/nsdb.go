/*

*/

package nsdb

import (
	"bytes"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"sync"

	"github.com/cznic/kv"
)

// Define namespaces for raw key sorted in db.
const (
	Empty byte = iota
	Meta
	KvKeyIndex
	KvItem
	KvItemMeta
	KvVersionCnt
	KvVersionMin
	KvVersionMax
)

var ErrNotFound = errors.New("nsdb: key does not exist")

type DB struct {
	db   *kv.DB
	path string
	mu   *sync.Mutex
}

// New creates a new database.
func New(path string) (*DB, error) {
	createOpen := kv.Open
	if _, err := os.Stat(path); os.IsNotExist(err) {
		createOpen = kv.Create
	}
	kvdb, err := createOpen(path, &kv.Options{})
	if err != nil {
		return nil, err
	}
	return &DB{
		db:   kvdb,
		path: path,
		mu:   new(sync.Mutex),
	}, nil
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

func encodeKey(hexHash, ns string) []byte {
	hash, err := hex.DecodeString(hexHash)
	if err != nil {
		panic(err)
	}
	var buf bytes.Buffer
	buf.Write(hash)
	buf.WriteString(ns)
	return buf.Bytes()
}

func (db *DB) AddNs(hexHash, ns string) error {
	return db.db.Set(encodeKey(hexHash, ns), []byte{})
}

func (db *DB) RemoveNs(hexHash, ns string) error {
	return db.db.Delete(encodeKey(hexHash, ns))
}

// Return a lexicographical range
func (db *DB) Namespaces(hexHash string) ([]string, error) {
	hash, err := hex.DecodeString(hexHash)
	if err != nil {
		return nil, err
	}
	res := []string{}

	enum, _, err := db.db.Seek(hash)
	if err != nil {
		return nil, err
	}
	endBytes := []byte(append(hash, '\xff'))
	for {
		k, _, err := enum.Next()
		if err == io.EOF {
			break
		}
		if bytes.Compare(k, endBytes) > 0 {
			return res, nil
		}
		res = append(res, string(k[32:]))
	}
	return res, nil
}
