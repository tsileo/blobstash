package docstore

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"

	"a4.io/blobstash/pkg/docstore/id"
	"a4.io/blobstash/pkg/vkv"
)

// Indexer is the interface that wraps the Index method
type Indexer interface {
	Index(id *id.ID, doc map[string]interface{}) error
	io.Closer
	IDIterator
}

type sortIndex struct {
	db     *vkv.DB
	fields []string
	name   string
}

func newSortIndex(name string, fields ...string) (*sortIndex, error) {
	// FIXME(tsileo): add the path
	db, err := vkv.New(fmt.Sprintf("docstore_%s.index", name))
	if err != nil {
		return nil, err
	}
	return &sortIndex{
		db:     db,
		name:   name,
		fields: fields,
	}, nil
}

func (si *sortIndex) Name() string {
	return si.name
}

func (si *sortIndex) prepareRebuild() error {
	err := si.db.Destroy()
	if err != nil {
		return err
	}
	si.db, err = vkv.New(fmt.Sprintf("docstore_%s.index", si.name))
	return err
}

func buildVal(start int64, _id *id.ID) []byte {
	v := make([]byte, 20) // start (8 byte int64) + 12 bytes ID
	binary.BigEndian.PutUint64(v[:], uint64(start))
	copy(v[8:], _id.Raw())
	return v
}

func parseVal(d []byte) (int64, *id.ID) {
	return int64(binary.BigEndian.Uint64(d[0:8])), id.FromRaw(d[8:])
}

func buildUint64Key(v uint64) []byte {
	k := make([]byte, 18) // 8 bytes uint64 + 4 bytes prefix (`k:<type>:`) and 6 bytes random suffix
	copy(k[:], []byte("k:1:"))
	binary.BigEndian.PutUint64(k[4:], v)
	return k
}

func buildFloat64Key(f float64) []byte {
	buf := new(bytes.Buffer)
	buf.WriteString("k:2:")
	// XXX: The rationale for using this instead `math.Float64bits` is to ensure the big endianess
	// (I'am afraid `math.Float64bits` endianess will depend on the arch)
	err := binary.Write(buf, binary.BigEndian, f)
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
		k = []byte("k:0:")
	case string:
		klen = len(vv)
		k = make([]byte, klen+10) // 4 bytes prefix (`k:<type>:`) and 6 bytes random suffix
		copy(k[:], []byte("k:3:"))
		copy(k[4:], []byte(vv))
	case int:
		klen = 8
		k = buildUint64Key(uint64(vv))
	case int8:
		klen = 8
		k = buildUint64Key(uint64(vv))
	case int16:
		klen = 8
		k = buildUint64Key(uint64(vv))
	case int32:
		klen = 8
		k = buildUint64Key(uint64(vv))
	case int64:
		klen = 8
		k = buildUint64Key(uint64(vv))
	case uint8:
		klen = 8
		k = buildUint64Key(uint64(vv))
	case uint16:
		klen = 8
		k = buildUint64Key(uint64(vv))
	case uint32:
		klen = 8
		k = buildUint64Key(uint64(vv))
	case uint64:
		klen = 8
		k = buildUint64Key(vv)
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
	oldSortKvKey, err := si.db.Get(string(lastVersionKey), -1)
	switch err {
	case nil:
		// There's an old key, fetch it
		oldSortKv, err := si.db.Get(string(oldSortKvKey.Data), -1)
		if err != nil {
			return err
		}
		if len(oldSortKv.Data) == 0 {
			break
		}
		_, _oid := parseVal(oldSortKv.Data)
		if _oid.String() != _id.String() {
			return fmt.Errorf("_id should match the old version key")
		}
		if err := si.db.Put(&vkv.KeyValue{
			Key:     oldSortKv.Key,
			Data:    oldSortKv.Data,
			Version: _id.Version(),
		}); err != nil {
			return err
		}
	case vkv.ErrNotFound:
	default:
		return err
	}

	if _id.Flag() == flagDeleted {
		return nil
	}

	var sortKey []byte
	if si.fields[0] == "_updated" {
		sortKey = buildKey(_id.Version())
	} else {
		sortKey = buildKey(doc[si.fields[0]])
	}

	if err := si.db.Put(&vkv.KeyValue{
		Key:     string(sortKey),
		Data:    buildVal(_id.Version(), _id),
		Version: _id.Version(),
	}); err != nil {
		return err
	}
	if err := si.db.Put(&vkv.KeyValue{
		Key:  string(lastVersionKey),
		Data: sortKey,
	}); err != nil {
		return err
	}

	return nil
}

// Iter implements the IDIterator interface
func (si *sortIndex) Iter(collection, cursor string, fetchLimit int, asOf int64) ([]*id.ID, string, error) {
	// asOfStr := strconv.FormatInt(asOf, 10)
	_ids := []*id.ID{}

	// Handle the cursor
	start := "k:\xff"
	if cursor != "" {
		decodedCursor, err := base64.URLEncoding.DecodeString(cursor)
		if err != nil {
			return nil, "", err
		}
		start = string(decodedCursor)
		// start = fmt.Sprintf("k:%s", decodedCursor)
	}

	// List keys from the kvstore
	res, nextCursor, err := si.db.ReverseKeys("k:", start, 50)
	if err != nil {
		return nil, "", err
	}
	var vstart int64
	var _id *id.ID

	for _, kv := range res {
		if len(kv.Data) == 0 {
			// Skip deleted entries
			continue
		}
		vstart, _id = parseVal(kv.Data)

		// We only want key for the latest version if asOf == 0
		if asOf == 0 && vstart != kv.Version {
			continue
		}

		if asOf > 0 && ((vstart == kv.Version && asOf < vstart) || (kv.Version > vstart && !(asOf >= vstart && asOf < kv.Version))) {
			// Skip documents created after the requested asOf, or document versions which are not between vstart and version
			continue
		}

		// Add the extra metadata to the ID
		_id.SetFlag(flagNoop)
		_id.SetVersion(vstart)
		_id.SetCursor(base64.URLEncoding.EncodeToString([]byte(vkv.PrevKey(kv.Key))))

		_ids = append(_ids, _id)
	}

	return _ids, base64.URLEncoding.EncodeToString([]byte(nextCursor)), nil
}

// Close implements io.Closer
func (si *sortIndex) Close() error {
	return si.db.Close()
}
