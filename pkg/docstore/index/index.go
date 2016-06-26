/*

Package index implements a basic index for the document store.

The index is built in a similar way that vkv (prefix handling).

For each indexed doc:

	IndexRow + "index:{collection}:{hash index}:{_id} => ""

{hash index} is the FNV 64a Hash of the index key-value.

*/
package index

import (
	"bytes"
	"errors"
	"fmt"
	_ "hash"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/tsileo/blobstash/pkg/config/pathutil"

	"github.com/cznic/kv"
)

// FIXME(tsileo): 1 kv file by index, nore more "index:{collection}:{index id}" preifx, just the index hash!

// TODO(tsileo):
// - Add way to remove an index
// - Store the index in the kvk store: index:{collection}:{index_id} => {Index Entry (json encoded)}

var DuplicateKeyError = errors.New("Duplicate key error")

// Define namespaces for raw key sorted in db.
const (
	Empty byte = iota
	IndexMeta
	IndexRowMeta
	IndexRow
	IndexPrefixMeta // Not in use yet but reserved for future usage
	IndexPrefixCnt  // Same here
	IndexPrefixStart
	IndexPrefixEnd
)

// FIXME(tsileo): remove the useless "index:" prefix on the IndexPrefixFmt

var (
	IndexPrefixFmt string = "index:%s:%v:%v"       // index:{collection}:{index id}:{index hash}
	IndexFmt       string = IndexPrefixFmt + ":%v" // index:{collection}:{index id}:{index hash}:{_id} => ""
)

type Index struct {
	ID     string   `json:"id,omitempty"`
	Fields []string `json:"fields"`
	Unique bool     `json:"unique,omitempty"`
}

// HashIndex will act as a basic indexing for basic queries like `{"key": "value"}`
type HashIndexes struct {
	db *kv.DB
}

func New() (*HashIndexes, error) {
	path := filepath.Join(pathutil.VarDir(), "docstore.index")
	createOpen := kv.Open
	if _, err := os.Stat(path); os.IsNotExist(err) {
		createOpen = kv.Create
	}
	db, err := createOpen(path, &kv.Options{})
	if err != nil {
		return nil, err
	}
	return &HashIndexes{
		db: db,
	}, nil
}

func IndexKey(value interface{}) string {
	h := fnv.New64a()
	// h.Write(key)
	h.Write([]byte(fmt.Sprintf("%v", value)))

	return fmt.Sprintf("%x", h.Sum(nil))
}

func encodeMeta(keyByte byte, key []byte) []byte {
	cardkey := make([]byte, len(key)+1)
	cardkey[0] = keyByte
	copy(cardkey[1:], key)
	return cardkey
}

func (hi *HashIndexes) Close() error {
	return hi.db.Close()
}

func (hi *HashIndexes) Index(collection string, index *Index, indexHash, _id string) error {
	prefixedKey := fmt.Sprintf(IndexPrefixFmt, collection, index.ID, indexHash)
	bprefix := []byte(prefixedKey)
	bkey := []byte(fmt.Sprintf(IndexFmt, collection, index.ID, indexHash, _id))
	bid := []byte(_id)

	// Check for "unique"ness if the index has an unique constraint
	if index.Unique {
		// exists, err := hi.db.Get(nil, encodeMeta(IndexRow, bkey))
		prefix := encodeMeta(IndexRow, []byte(fmt.Sprintf(IndexFmt, collection, index.ID, indexHash, "")))
		enum, _, err := hi.db.Seek(prefix)
		if err != nil {
			return err
		}
		k, _, err := enum.Next()
		switch err {
		case io.EOF:
		case nil:
			if strings.HasPrefix(string(k), string(prefix)) {
				return DuplicateKeyError
			}
		default:
			return err
		}
	}

	// Track the boundaries for later iteration
	prefixStart, err := hi.db.Get(nil, encodeMeta(IndexPrefixStart, bprefix))
	if err != nil {
		return err
	}
	if (prefixStart == nil || len(prefixStart) == 0) || bytes.Compare(bid, prefixStart) < 0 {
		if err := hi.db.Set(encodeMeta(IndexPrefixStart, bprefix), bid); err != nil {
			return err
		}
	}
	prefixEnd, err := hi.db.Get(nil, encodeMeta(IndexPrefixEnd, bprefix))
	if err != nil {
		return err
	}
	if (prefixEnd == nil || len(prefixEnd) == 0) || bytes.Compare(bid, prefixEnd) > 0 {
		if err := hi.db.Set(encodeMeta(IndexPrefixEnd, bprefix), bid); err != nil {
			return err
		}
	}

	// Set the actual index row
	// FIXME(tsileo): set value to nil instead of 1 and check if the boundaries are really needed
	return hi.db.Set(encodeMeta(IndexRow, bkey), []byte{1})
}

func (hi *HashIndexes) Iter(collection string, index *Index, indexHash, start, end string, limit int) ([]string, error) {
	prefixedKey := fmt.Sprintf(IndexPrefixFmt, collection, index.ID, indexHash)
	bprefix := []byte(prefixedKey)

	res := []string{}
	if start == "" {
		prefixStart, err := hi.db.Get(nil, encodeMeta(IndexPrefixStart, bprefix))
		if err != nil {
			return nil, err
		}
		// If there's no boundary, there's no index for this key/value pair
		if prefixStart == nil || len(prefixStart) == 0 {
			return res, nil
		}
		start = string(prefixStart)
	}
	// FIXME(tsileo): a better way to tell we want the end? or \xff is good enough?
	if end == "\xff" {
		prefixEnd, err := hi.db.Get(nil, encodeMeta(IndexPrefixEnd, bprefix))
		if err != nil {
			return nil, err
		}
		// If there's no boundary, there's no index for this key/value pair
		if prefixEnd == nil || len(prefixEnd) == 0 {
			return res, nil
		}
		end = string(prefixEnd)
	}
	enum, _, err := hi.db.Seek(encodeMeta(IndexRow, []byte(fmt.Sprintf(IndexFmt, collection, index.ID, indexHash, end))))
	if err != nil {
		return nil, err
	}
	endBytes := encodeMeta(IndexRow, []byte(fmt.Sprintf(IndexFmt, collection, index.ID, indexHash, start)))
	i := 0
	for {
		k, _, err := enum.Prev()
		if err == io.EOF {
			break
		}
		if bytes.Compare(k, endBytes) < 0 || (limit != 0 && i > limit) {
			return res, nil
		}
		// XXX(tsileo): is the extra check really necessary?
		if !strings.HasPrefix(string(k[1:]), fmt.Sprintf(IndexFmt, collection, index.ID, indexHash, "")) {
			break
		}
		// fmt.Printf("\n\n%s\n%s\n\n\n", k[1:], fmt.Sprintf(IndexFmt, collection, index.ID, indexHash, ""))
		_id := strings.Replace(string(k[1:]), fmt.Sprintf(IndexFmt, collection, index.ID, indexHash, ""), "", 1)
		res = append(res, _id)
		i++
	}
	return res, nil
}