package blobsfile

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/cznic/kv"
)

const (
	MetaKey byte = iota
	BlobPosKey
)

func formatKey(prefix byte, key string) []byte {
	bkey := []byte(key)
	res := make([]byte, len(bkey)+1)
	res[0] = prefix
	copy(res[1:], bkey)
	return res
}

func opts() *kv.Options {
	return &kv.Options{
		VerifyDbBeforeOpen:  true,
		VerifyDbAfterOpen:   true,
		VerifyDbBeforeClose: true,
		VerifyDbAfterClose:  true,
	}
}

type BlobsIndex struct {
	db   *kv.DB
	path string
	sync.Mutex
}

type BlobPos struct {
	// bobs-n files
	n int
	// blobs offset/size in the blobs file
	offset int
	size   int
}

func (blob BlobPos) String() string {
	return fmt.Sprintf("%v %v %v", blob.n, blob.offset, blob.size)
}

func ScanBlobPos(s string) (blob BlobPos, error error) {
	n, err := fmt.Sscan(s, &blob.n, &blob.offset, &blob.size)
	if n != 3 || err != nil {
		return blob, err
	}
	return blob, nil
}

// NewIndex initialize a new index.
func NewIndex(path string) (*BlobsIndex, error) {
	db_path := filepath.Join(path, "blobs-index")
	if err := os.MkdirAll(path, 0700); err != nil {
		return nil, err
	}
	createOpen := kv.Open
	if _, err := os.Stat(db_path); os.IsNotExist(err) {
		createOpen = kv.Create
	}
	db, err := createOpen(db_path, opts())
	return &BlobsIndex{db: db, path: db_path}, err
}

// Close cleanly close the kv db.
func (index *BlobsIndex) Close() {
	index.Lock()
	defer index.Unlock()
	index.db.Close()
}

// Remove remove the kv file
func (index *BlobsIndex) Remove() {
	log.Printf("removing path: %v", index.path)
	os.RemoveAll(index.path)
}

func (index *BlobsIndex) SetPos(hash string, pos BlobPos) error {
	index.Lock()
	defer index.Unlock()
	return index.db.Set(formatKey(BlobPosKey, hash), []byte(pos.String()))
}

func (index *BlobsIndex) GetPos(hash string) (*BlobPos, error) {
	index.Lock()
	defer index.Unlock()
	data, err := index.db.Get(nil, formatKey(BlobPosKey, hash))
	if err != nil {
		return nil, fmt.Errorf("error getting BlobPos: %v", err)
	}
	if data == nil {
		return nil, nil
	}
	bpos, err := ScanBlobPos(string(data))
	return &bpos, err
}

func (index *BlobsIndex) SetN(n int) error {
	index.Lock()
	defer index.Unlock()
	return index.db.Set(formatKey(MetaKey, "n"), []byte(strconv.Itoa(n)))
}

func (index *BlobsIndex) GetN() (int, error) {
	index.Lock()
	defer index.Unlock()
	data, err := index.db.Get(nil, formatKey(MetaKey, "n"))
	if err != nil || string(data) == "" {
		return 0, nil
	}
	return strconv.Atoi(string(data))
}
