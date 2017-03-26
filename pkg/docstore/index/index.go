/*

Package index implements a basic index for the document store.

The index is built in a similar way that vkv (prefix handling).

For each indexed doc:

	IndexRow + "index:{collection}:{hash index}:{_id} => ""
	IndexRow + {hash index} + {12 random bytes} => _id

{hash index} is the FNV 64a Hash of the index key-value.

*/
package index // import "a4.io/blobstash/pkg/docstore/index"

import (
	"bytes"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"
	"sort"

	"a4.io/blobstash/pkg/config"

	"github.com/cznic/kv"
)

// FIXME(tsileo): 1 kv file by index, nore more "index:{collection}:{index id}" preifx, just the index hash!

// TODO(tsileo):
// - Add way to remove an index

var DuplicateKeyError = errors.New("Duplicate key error")

// Define namespaces for raw key sorted in db.
const (
	Empty byte = iota
	IndexMeta
	IndexRowMeta
	IndexRow
	IndexPrefixMeta // Not in use yet but reserved for future usage
	IndexPrefixCnt  // Same here
)

// FIXME(tsileo): remove the useless "index:" prefix on the IndexPrefixFmt

var (
	IndexPrefixFmt string = "index:%s:%v:%v"       // index:{collection}:{index id}:{index hash}
	IndexFmt       string = IndexPrefixFmt + ":%v" // index:{collection}:{index id}:{index hash}:{_id} => ""
)

type Index struct {
	Fields []string `json:"fields"`
	// Unique bool     `json:"unique,omitempty"`  // FIXME(tsileo): support unique?
	Sort []string `json:"sort"`
}

func idFromFields(fields []string) string {
	id := ""
	for _, f := range fields {
		id += fmt.Sprintf("%v-", f)
	}
	return id[:len(id)-1]
}

func (i *Index) ID() string {
	return idFromFields(i.Fields)
}

type IndexValues []interface{}

func (v IndexValues) Hash(index *Index) string {
	h := fnv.New64a()
	for i, val := range v {
		h.Write([]byte("key:"))
		h.Write([]byte(index.Fields[i]))
		h.Write([]byte("value:"))
		h.Write([]byte(fmt.Sprintf("%v", val)))
	}
	return fmt.Sprintf("%x", h.Sum(nil))

}

// HashIndex will act as a basic indexing for basic queries like `{"key": "value"}`
type HashIndex struct {
	db     *kv.DB
	config *config.Config
	Path   string
	index  *Index
}

func New(conf *config.Config, index *Index) (*HashIndex, error) {
	// FIXME(tsileo): should use `config.VarDir()`
	sort.Strings(index.Fields)
	if index.Sort == nil || len(index.Sort) == 0 {
		index.Sort = append(index.Sort, "_id")
	}
	sort.Strings(index.Sort)
	indexName := fmt.Sprintf("docstore.%s.%s.index", idFromFields(index.Fields), idFromFields(index.Sort))
	path := filepath.Join(conf.VarDir(), indexName)
	createOpen := kv.Open
	if _, err := os.Stat(path); os.IsNotExist(err) {
		createOpen = kv.Create
	}
	db, err := createOpen(path, &kv.Options{})
	if err != nil {
		return nil, err
	}
	return &HashIndex{
		db:     db,
		index:  index,
		Path:   path,
		config: conf,
	}, nil
}

func encodeMeta(keyByte byte, key []byte) []byte {
	cardkey := make([]byte, len(key)+1)
	cardkey[0] = keyByte
	copy(cardkey[1:], key)
	return cardkey
}

func (hi *HashIndex) Close() error {
	return hi.db.Close()
}

// TODO(tsileo): support alternative sort key!
func (hi *HashIndex) Index(idxValues IndexValues, _id string) error {
	// TODO(tsileo): the _id could be hex decoded to save a few bytes
	k := make([]byte, 16+24)
	copy(k[:], idxValues.Hash(hi.index))
	copy(k[16:], []byte(_id))
	if err := hi.db.Set(encodeMeta(IndexRow, k), []byte(_id)); err != nil {
		return err
	}

	return nil
}

func (hi *HashIndex) IterReverse(idxValues IndexValues, start, end string, limit int) ([]string, error) {
	// TODO(tsileo): be able to switch between Iter/IterReverse (i.e. the sort order)
	h := []byte(idxValues.Hash(hi.index))
	enum, err := hi.db.SeekLast() // (encodeMeta(IndexRow, bend))
	if err != nil {
		return nil, err
	}
	var res []string
	bstart := append(h, []byte(start)...)
	endBytes := encodeMeta(IndexRow, bstart)
	i := 0
	for {
		k, _id, err := enum.Prev()
		if err == io.EOF {
			break
		}
		if bytes.Compare(k, endBytes) < 0 || (limit != 0 && i > limit) {
			return res, nil
		}
		res = append(res, string(_id))
		i++
	}

	return res, nil
}
