package docstore

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"path/filepath"
	"time"

	log "github.com/inconshreveable/log15"
	logext "github.com/inconshreveable/log15/ext"

	"a4.io/blobstash/pkg/config"
	"a4.io/blobstash/pkg/docstore/id"
	"a4.io/blobstash/pkg/docstore/maputil"
	"a4.io/blobstash/pkg/rangedb"
	"a4.io/blobstash/pkg/vkv"
)

// Indexer is the interface that wraps the Index method
type Indexer interface {
	Index(id *id.ID, doc map[string]interface{}) error
	io.Closer
	IDIterator
}

// sortIndex implements a "temporal" single-field index.
// The index can be traversed in either direction (i.e. support ascending/descending sort order out of the box).
// It stores the value of a specific field , ordered by its value.
// When comparing different types, the following comparison order is used:
//  1. null values
//  2. numbers (ints and floats)
//  3. string
//  4. bool
// The index is "temporal" because each document version is indexed with (start, end) timestamp that
// specifies the lifetime of the indexed document (start == end means it's the latest version).
// An additional "sub-index" is kept in roder to keep track of the "index key" of the latest version of each document.
type sortIndex struct {
	db                *rangedb.RangeDB
	conf              *config.Config
	field, collection string
	logger            log.Logger
}

func newSortIndex(logger log.Logger, conf *config.Config, collection, field string) (*sortIndex, error) {
	db, err := rangedb.New(filepath.Join(conf.VarDir(), fmt.Sprintf("docstore_%s_%s.index", collection, field)))
	if err != nil {
		return nil, err
	}
	return &sortIndex{
		db:         db,
		field:      field,
		collection: collection,
		conf:       conf,
		logger:     logger.New("index", fmt.Sprintf("sf:%s:%s", collection, field)),
	}, nil
}

func (si *sortIndex) Name() string {
	return fmt.Sprintf("sf:%s:%s", si.collection, si.field)
}

func (si *sortIndex) prepareRebuild() error {
	err := si.db.Destroy()
	if err != nil {
		return err
	}
	si.db, err = rangedb.New(filepath.Join(si.conf.VarDir(), fmt.Sprintf("docstore_%s_%s.index", si.collection, si.field)))
	return err
}

func buildVal(start, end int64, _id *id.ID) []byte {
	v := make([]byte, 28) // start + end (2 * 8 byte int64) + 12 bytes ID
	binary.BigEndian.PutUint64(v[:], uint64(start))
	binary.BigEndian.PutUint64(v[8:], uint64(end))
	copy(v[16:], _id.Raw())
	return v
}

func parseVal(d []byte) (int64, int64, *id.ID) {
	return int64(binary.BigEndian.Uint64(d[0:8])), int64(binary.BigEndian.Uint64(d[8:16])), id.FromRaw(d[16:])
}

func buildFloat64Key(f float64) []byte {
	buf := new(bytes.Buffer)
	buf.WriteString("k:1:")
	// Get the IEEE-754 binary version of this float
	bits := math.Float64bits(f)
	if f >= 0 {
		// Flip the sign part of the IEEE-754 repr
		bits ^= 0x8000000000000000
	} else {
		// Flip the sign part and reverse the ordering for negative numbers by flipping the bits
		bits ^= 0xffffffffffffffff
	}
	err := binary.Write(buf, binary.BigEndian, bits)
	if err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func buildKey(v interface{}) []byte {
	var k []byte
	var klen int
	switch vv := v.(type) {
	case nil:
		klen = 0
		k = make([]byte, 10)
		copy(k[:], []byte("k:0:"))
	case bool:
		klen = 0
		k = make([]byte, 10)
		if vv {
			copy(k[:], []byte("k:3:1"))
		} else {
			copy(k[:], []byte("k:3:0"))
		}
	case string:
		klen = len(vv)
		k = make([]byte, klen+10) // 4 bytes prefix (`k:<type>:`) and 6 bytes random suffix
		copy(k[:], []byte("k:2:"))
		copy(k[4:], []byte(vv))
	case int:
		klen = 8
		k = buildFloat64Key(float64(vv))
	case int8:
		klen = 8
		k = buildFloat64Key(float64(vv))
	case int16:
		klen = 8
		k = buildFloat64Key(float64(vv))
	case int32:
		klen = 8
		k = buildFloat64Key(float64(vv))
	case int64:
		klen = 8
		k = buildFloat64Key(float64(vv))
	case uint8:
		klen = 8
		k = buildFloat64Key(float64(vv))
	case uint16:
		klen = 8
		k = buildFloat64Key(float64(vv))
	case uint32:
		klen = 8
		k = buildFloat64Key(float64(vv))
	case uint64:
		klen = 8
		k = buildFloat64Key(float64(vv))
	case float32:
		klen = 8
		// Get the IEEE 754 binary repr
		k = buildFloat64Key(float64(vv))
	case float64:
		klen = 8
		// Get the IEEE 754 binary repr
		k = buildFloat64Key(vv)
	case []interface{}:
		panic("TODO support slice")
	default:
		panic("should not happen")
	}
	if _, err := rand.Read(k[klen+4:]); err != nil {
		panic("failed to build key")
	}
	return k
}

func buildLastVersionKey(_id *id.ID) []byte {
	k := make([]byte, 14) // 2 bytes prefix (`v:`) + 12 bytes ID
	copy(k[:], []byte("v:"))
	copy(k[2:], _id.Raw())
	return k
}

// Index implements the Indexer interface
func (si *sortIndex) Index(_id *id.ID, doc map[string]interface{}) error {
	lastVersionKey := buildLastVersionKey(_id)
	oldSortKvKey, err := si.db.Get(lastVersionKey)
	switch err {
	case nil:
		if oldSortKvKey == nil {
			break
		}
		// There's an old key, fetch it
		oldSortKv, err := si.db.Get(oldSortKvKey)
		if err != nil {
			return err
		}
		if oldSortKv == nil || len(oldSortKv) == 0 {
			break
		}
		start, _, _oid := parseVal(oldSortKv)
		if _oid.String() != _id.String() {
			return fmt.Errorf("_id should match the old version key")
		}
		// And update its "end of life" date (the newer doc version's version)
		if err := si.db.Set(oldSortKvKey, buildVal(start, _id.Version(), _oid)); err != nil {
			return err
		}
	default:
		return err
	}

	// If the index is updated with a deleted doc, updating the end of life of the last/previous version (done above) is enough
	if _id.Flag() == flagDeleted {
		return nil
	}

	// Build the "index key", the encoded value (for later lexicographical iter)
	var sortKey []byte
	if si.field == "_updated" {
		sortKey = buildKey(_id.Version())
	} else {
		val, _ := maputil.GetPath(doc, si.field)
		sortKey = buildKey(val)
	}

	// Append the "index key", since it's the latest version, end == max int64
	if err := si.db.Set(sortKey, buildVal(_id.Version(), math.MaxInt64, _id)); err != nil {
		return err
	}

	// Update the pointer to the latest index key (to update its end of life when a newer version comes in)
	if err := si.db.Set(lastVersionKey, sortKey); err != nil {
		return err
	}

	return nil
}

type kv struct {
	k []byte
	v []byte
}

func (si *sortIndex) keys(start, end string, limit int, reverse bool) ([]*kv, string, error) {
	var cursor string
	out := []*kv{}

	c := si.db.Range([]byte(start), []byte(end), reverse)
	defer c.Close()

	// Iterate the range
	k, v, err := c.Next()
	for ; err == nil && (limit <= 0 || len(out) < limit); k, v, err = c.Next() {
		res := &kv{k: k, v: v}
		out = append(out, res)
	}

	if len(out) > 0 {
		// Generate next cursor
		rcursor := string(out[len(out)-1].k)
		if reverse {
			cursor = vkv.PrevKey(rcursor)
		} else {
			cursor = vkv.NextKey(rcursor)
		}
	}

	// Return
	if err == io.EOF {
		return out, cursor, nil
	}

	return out, cursor, nil
}

// Iter implements the IDIterator interface
func (si *sortIndex) Iter(collection, cursor string, desc bool, fetchLimit int, asOf int64) ([]*id.ID, string, error) {
	tstart := time.Now()
	l := si.logger.New("id", logext.RandId(8))
	l.Debug("starting iter")
	var scanned int

	// asOfStr := strconv.FormatInt(asOf, 10)
	_ids := []*id.ID{}

	// Handle the cursor (and the sort order)
	var start string
	var nextFunc func(string) string
	if desc {
		start = "k:\xff"
		nextFunc = vkv.PrevKey
	} else {
		start = "k:"
		nextFunc = vkv.NextKey
	}
	if cursor != "" {
		decodedCursor, err := base64.URLEncoding.DecodeString(cursor)
		if err != nil {
			return nil, "", err
		}
		start = string(decodedCursor)
	}

	// List keys from the kvstore
	var res []*kv
	var err error
	var nextCursor string

	if desc {
		res, nextCursor, err = si.keys("k:", start, fetchLimit, true)
	} else {
		res, nextCursor, err = si.keys(start, "k:\xff", fetchLimit, false)
	}
	if err != nil {
		return nil, "", err
	}
	var vstart, vend int64
	var _id *id.ID

	for _, kv := range res {
		scanned++

		vstart, vend, _id = parseVal(kv.v)

		// Skip doc if the latest version is requested and this is not the latest version
		// Or if the current doc is not between start and end
		if (asOf == 0 && vend != math.MaxInt64) || (asOf > 0 && !(asOf >= vstart && asOf < vend)) {
			continue
		}

		// Add the extra metadata to the ID
		_id.SetFlag(flagNoop)
		_id.SetVersion(vstart)
		// Cursor is needed by ID as we don't know yet which doc will be matched, and and want to return in the query
		_id.SetCursor(base64.URLEncoding.EncodeToString([]byte(nextFunc(string(kv.k)))))

		_ids = append(_ids, _id)
	}

	l.Debug("iter done", "duration", time.Since(tstart), "scanned", scanned, "count", len(_ids))

	return _ids, base64.URLEncoding.EncodeToString([]byte(nextCursor)), nil
}

// Close implements io.Closer
func (si *sortIndex) Close() error {
	return si.db.Close()
}
