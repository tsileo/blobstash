package blobsfile

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/cznic/kv"
)

// MetaKey and BlobPosKey are used to namespace the DB keys.
const (
	MetaKey byte = iota
	BlobPosKey
)

// Add the prefix byte to the given key
func formatKey(prefix byte, bkey []byte) []byte {
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

// BlobsIndex holds the position of blobs in BlobsFile
type BlobsIndex struct {
	db   *kv.DB
	path string
	sync.Mutex
}

// BlobPos is a blob entry in the index
type BlobPos struct {
	// bobs-n files
	n int
	// blobs offset/size in the blobs file
	offset int
	size   int
}

func (blob *BlobPos) Size() int {
	return blob.size
}

// Value serialize a BlobsPos as string
// (value is encoded as uvarint: n + offset + size)
func (blob BlobPos) Value() []byte {
	bufTmp := make([]byte, 10)
	var buf bytes.Buffer
	w := binary.PutUvarint(bufTmp[:], uint64(blob.n))
	buf.Write(bufTmp[:w])
	w = binary.PutUvarint(bufTmp[:], uint64(blob.offset))
	buf.Write(bufTmp[:w])
	w = binary.PutUvarint(bufTmp[:], uint64(blob.size))
	buf.Write(bufTmp[:w])
	return buf.Bytes()
}

func decodeBlobPos(data []byte) (blob BlobPos, error error) {
	r := bytes.NewBuffer(data)
	// read blob.n
	ures, err := binary.ReadUvarint(r)
	if err != nil {
		return blob, err
	}
	blob.n = int(ures)

	// read blob.offset
	ures, err = binary.ReadUvarint(r)
	if err != nil {
		return blob, err
	}
	blob.offset = int(ures)

	// read blob.size
	ures, err = binary.ReadUvarint(r)
	if err != nil {
		return blob, err
	}
	blob.size = int(ures)
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
	db, err := createOpen(db_path, &kv.Options{})
	return &BlobsIndex{db: db, path: db_path}, err
}

func (index *BlobsIndex) FormatBlobPosKey(key string) []byte {
	return formatKey(BlobPosKey, []byte(key))
}

func (index *BlobsIndex) DB() *kv.DB {
	return index.db
}

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

// SetPos creates a new BlobPos entry in the index for the given hash.
func (index *BlobsIndex) SetPos(hexHash string, pos *BlobPos) error {
	index.Lock()
	defer index.Unlock()
	hash, err := hex.DecodeString(hexHash)
	if err != nil {
		return err
	}
	return index.db.Set(formatKey(BlobPosKey, hash), pos.Value())
}

// DeletePos deletes the stored BlobPos for the given hash.
func (index *BlobsIndex) DeletePos(hexHash string) error {
	index.Lock()
	defer index.Unlock()
	hash, err := hex.DecodeString(hexHash)
	if err != nil {
		return err
	}
	return index.db.Delete(formatKey(BlobPosKey, hash))
}

// GetPos retrieve the stored BlobPos for the given hash.
func (index *BlobsIndex) GetPos(hexHash string) (*BlobPos, error) {
	index.Lock()
	defer index.Unlock()
	hash, err := hex.DecodeString(hexHash)
	if err != nil {
		return nil, err
	}
	data, err := index.db.Get(nil, formatKey(BlobPosKey, hash))
	if err != nil {
		return nil, fmt.Errorf("error getting BlobPos: %v", err)
	}
	if data == nil {
		return nil, nil
	}
	bpos, err := decodeBlobPos(data)
	return &bpos, err
}

// SetN stores the latest N (blobs-N) to remember the latest BlobsFile opened.
func (index *BlobsIndex) SetN(n int) error {
	index.Lock()
	defer index.Unlock()
	return index.db.Set(formatKey(MetaKey, []byte("n")), []byte(strconv.Itoa(n)))
}

// GetN retrieves the latest N (blobs-N) stored.
func (index *BlobsIndex) GetN() (int, error) {
	index.Lock()
	defer index.Unlock()
	data, err := index.db.Get(nil, formatKey(MetaKey, []byte("n")))
	if err != nil || string(data) == "" {
		return 0, nil
	}
	return strconv.Atoi(string(data))
}
