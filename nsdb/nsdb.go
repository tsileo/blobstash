/*

 */

package nsdb

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/cznic/kv"
)

type DB struct {
	db   *kv.DB
	path string
	sync.Mutex
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
	buf.WriteString(ns + ":")
	buf.Write(hash)
	return buf.Bytes()
}

func (db *DB) AddNs(hexHash, ns string) error {
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

func (db *DB) ApplyMeta(hash string) error {
	return db.db.Set([]byte(fmt.Sprintf("_meta:%s", hash)), []byte("1"))
}

func (db *DB) BlobApplied(hash string) (bool, error) {
	res, err := db.db.Get(nil, []byte(fmt.Sprintf("_meta:%s", hash)))
	if err != nil {
		return false, err
	}
	if res == nil || len(res) == 0 {
		return false, nil
	}
	return true, nil
}
