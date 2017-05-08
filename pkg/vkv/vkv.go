/*

Package vkv implements a versioned key value store.

The change history for all keys are kept and versioned by timestamp.

Keys are sorted lexicographically.

## How data is stored ?

Key index (for faster exists check):

    KvKeyIndex + key => 1

Meta index (to track the serialized meta blob for each version of a key, and be able to remove it)

    KvItemMeta + key + version => hash of meta blob

Key value:

    KvItem + (key length as binary encoded uint32) + kv key + index (uint64) => value

Version boundaries for each kv the min/max version is stored for iteration/seeking:

    Meta + KvVersionMin/KvVersionMax + kv key => binary encoded uint64

Number of versions for each key:

    Meta + KvVersionCnt + kv key => binary encoded uint64


*/
package vkv // import "a4.io/blobstash/pkg/vkv"

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/cznic/kv"
)

// Define namespaces for raw key sorted in db.
const (
	_ byte = iota
	KvKeyIndex
	KvItem
)

// KvType for meta serialization
const KvType = "kv"

type Flag byte

const (
	Unknown Flag = iota
	Deleted
	HashOnly
	DataOnly
	HashAndData
)

var ErrNotFound = errors.New("vkv: key does not exist")

// KeyValueVersions holds the full history for a key value pair
type KeyValueVersions struct {
	Key string `json:"key"`

	// FIXME(tsileo): turn this into a []*VkvEntry
	Versions []*KeyValue `json:"versions"`
}

// NextKey returns the next key for lexigraphical (key = NextKey(lastkey))
func NextKey(key string) string {
	bkey := []byte(key)
	i := len(bkey)
	for i > 0 {
		i--
		bkey[i]++
		if bkey[i] != 0 {
			break
		}
	}
	return string(bkey)
}

// PextKey returns the next key for lexigraphical (key = PextKey(lastkey))
func PrevKey(key string) string {
	bkey := []byte(key)
	i := len(bkey)
	for i > 0 {
		i--
		bkey[i]--
		if bkey[i] != 255 {
			break
		}
	}
	return string(bkey)
}

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

func encodeKey(key []byte, version int) []byte {
	versionbyte := make([]byte, 8)
	binary.BigEndian.PutUint64(versionbyte, uint64(version))
	k := make([]byte, len(key)+13)
	k[0] = KvItem
	binary.LittleEndian.PutUint32(k[1:5], uint32(len(key)))
	copy(k[5:], key)
	copy(k[5+len(key):], versionbyte)
	return k
}

// Extract the index from the raw key
func decodeKey(key []byte) (string, int) {
	klen := int(binary.LittleEndian.Uint32(key[1:5]))
	index := int(binary.BigEndian.Uint64(key[len(key)-8:]))
	member := make([]byte, klen)
	copy(member, key[5:5+klen])
	return string(member), index
}

func encodeMeta(keyByte byte, key []byte) []byte {
	cardkey := make([]byte, len(key)+1)
	cardkey[0] = keyByte
	copy(cardkey[1:], key)
	return cardkey
}

// Put updates the value for the given version associated with key,
// if version == -1, version will be set to time.Now().UTC().UnixNano().
func (db *DB) Put(key, ref string, data []byte, version int) (*KeyValue, error) {
	if version < 1 {
		version = int(time.Now().UTC().UnixNano())
	}
	bkey := []byte(key)
	kmember := encodeKey(bkey, version)
	kv := &KeyValue{
		Hash:    ref,
		Data:    data,
		Version: version,
		Key:     key,
		db:      db,
	}
	value, err := kv.Serialize(false)
	if err != nil {
		return nil, err
	}
	if err := db.db.Set(kmember, []byte(value)); err != nil {
		return nil, err
	}
	if err := db.db.Set(encodeMeta(KvKeyIndex, bkey), kmember); err != nil {
		return nil, err
	}
	return kv, nil
}

func (db *DB) Check(key string) (bool, error) {
	bkey := []byte(key)
	exists, err := db.db.Get(nil, encodeMeta(KvKeyIndex, bkey))
	if err != nil {
		return false, err
	}
	if len(exists) == 0 {
		return false, nil
	}
	return true, nil
}

// Get returns the latest value for the given key,
// if version < 1, the latest version will be returned.
func (db *DB) Get(key string, version int) (*KeyValue, error) {
	bkey := []byte(key)
	exists, err := db.db.Get(nil, encodeMeta(KvKeyIndex, bkey))
	if err != nil {
		return nil, fmt.Errorf("index key lookup failed: %v", err)
	}
	if len(exists) == 0 {
		return nil, ErrNotFound
	}
	var k []byte
	if version < 1 {
		k, err = db.db.Get(nil, encodeMeta(KvKeyIndex, bkey))
		if err != nil {
			return nil, fmt.Errorf("failed to get max version: %v", err)
		}
		version = int(binary.BigEndian.Uint64(k[len(k)-8:]))
	} else {
		k = encodeKey(bkey, version)
	}
	val, err := db.db.Get(nil, k)
	if err != nil {
		return nil, fmt.Errorf("failed to get key \"%s\": %v", encodeKey(bkey, version), err)
	}
	kv, err := Unserialize(key, val)
	if err != nil {
		return nil, fmt.Errorf("failed to unserialize key: %v", err)
	}
	kv.Version = version

	return kv, nil
}

// Return the versions in reverse lexicographical order
func (db *DB) Versions(key string, start, end, limit int) (*KeyValueVersions, int, error) { // TODO(tsileo): return a "new start" int like in Keys
	var nstart int
	res := &KeyValueVersions{
		Key:      key,
		Versions: []*KeyValue{},
	}
	bkey := []byte(key)
	exists, err := db.db.Get(nil, encodeMeta(KvKeyIndex, bkey))
	if err != nil {
		return nil, nstart, err
	}
	if len(exists) == 0 {
		return nil, nstart, ErrNotFound
	}
	if start < 1 {
		start = int(time.Now().UTC().UnixNano())
	}
	enum, _, err := db.db.Seek(encodeKey(bkey, start))
	if err != nil {
		return nil, nstart, err
	}
	endBytes := encodeKey(bkey, end)
	i := 1
	skipOnce := true
	eofOnce := true
	for {
		k, v, err := enum.Prev()
		if err == io.EOF {
			if eofOnce {
				enum, err = db.db.SeekLast()
				if err != nil {
					return nil, nstart, err
				}
				eofOnce = false
				continue
			}
			break
		}
		if bytes.Equal(k, encodeKey(bkey, start)) {
			continue
		}
		if bytes.Compare(k, endBytes) < 1 || len(endBytes) < 8 || !bytes.HasPrefix(k, endBytes[0:len(endBytes)-8]) || (limit > 0 && i > limit) {
			if skipOnce {
				skipOnce = false
				continue
			}
			return res, nstart, nil
		}
		_, index := decodeKey(k)
		kv, err := Unserialize(key, v)
		if err != nil {
			return nil, nstart, fmt.Errorf("failed to unserialize: %v", err)
		}
		kv.Version = index
		res.Versions = append(res.Versions, kv)
		nstart = index
		i++
	}
	return res, nstart, nil
}

// Return a lexicographical range
func (db *DB) Keys(start, end string, limit int) ([]*KeyValue, string, error) {
	var next string
	res := []*KeyValue{}
	enum, _, err := db.db.Seek(encodeMeta(KvKeyIndex, []byte(start)))
	if err != nil {
		return nil, next, fmt.Errorf("initial seek error: %v", err)
	}
	endBytes := encodeMeta(KvKeyIndex, []byte(end))
	i := 1
	for {
		k, k2, err := enum.Next()
		if err == io.EOF {
			break
		}
		if k[0] != KvKeyIndex || bytes.Compare(k, endBytes) > 0 || (limit > 0 && i > limit) {
			return res, next, nil
		}
		version := int(binary.BigEndian.Uint64(k2[len(k2)-8:]))
		val, err := db.db.Get(nil, k2)
		if err != nil {
			return nil, next, err
		}
		kv, err := Unserialize(string(k[1:]), val)
		if err != nil {
			return nil, next, err
		}
		kv.Version = version
		res = append(res, kv)
		next = NextKey(kv.Key)
		i++
	}
	return res, next, nil
}

// Return a lexicographical range
func (db *DB) ReverseKeys(start, end string, limit int) ([]*KeyValue, string, error) {
	var prev string
	res := []*KeyValue{}
	startBytes := encodeMeta(KvKeyIndex, []byte(start))
	enum, _, err := db.db.Seek(startBytes)
	if err != nil {
		return nil, prev, err
	}
	endBytes := encodeMeta(KvKeyIndex, []byte(end))
	i := 1
	skipOnce := true
	for {
		k, v, err := enum.Prev()
		if err == io.EOF {
			break
		}
		if k[0] != KvKeyIndex || bytes.Compare(k, startBytes) > 0 || bytes.Compare(k, endBytes) < 0 || (limit > 0 && i > limit) {
			if skipOnce {
				skipOnce = false
				continue
			}
			return res, prev, nil
		}
		version := int(binary.BigEndian.Uint64(v[len(v)-8:]))
		val, err := db.db.Get(nil, v)
		if err != nil {
			return nil, prev, err
		}
		kv, err := Unserialize(string(k[1:]), val)
		if err != nil {
			return nil, prev, err
		}
		kv.Version = version
		res = append(res, kv)
		prev = PrevKey(kv.Key)
		i++
	}
	return res, prev, nil
}

type KeyValue struct {
	Flag Flag `json:"flag"`
	// CustomFlag byte `json:"cflag"`

	Key     string `json:"key,omitempty"`
	Version int    `json:"version"`
	db      *DB

	Hash string `json:"hash,omitempty"`
	Data []byte `json:"data,omitempty"`
}

func (ve *KeyValue) Type() string {
	return KvType
}

// Implements the `MetaData` interface
func (ve *KeyValue) Dump() ([]byte, error) {
	return ve.Serialize(true)
}

// Implements the `MetaData` interface
func (ve *KeyValue) Serialize(withKey bool) ([]byte, error) {
	// Check if the flag is set
	if ve.Flag == Unknown {
		if ve.Hash != "" && ve.Data != nil {
			ve.Flag = HashAndData
		} else if ve.Hash != "" {
			ve.Flag = HashOnly
		} else if ve.Data != nil {
			ve.Flag = DataOnly
		}
	}

	// XXX(tsileo): find a way to do the GC using data via another ExtractFunc interface that returns a list of []Ref?
	// e.g. parse the filetreemeta recursively until we discover all the blobs?

	var buf bytes.Buffer
	// Store the version byte
	if err := buf.WriteByte(0); err != nil {
		return nil, err
	}

	// Store the 1 byte flag
	if err := buf.WriteByte(byte(ve.Flag)); err != nil {
		return nil, err
	}

	tmp := make([]byte, 4)
	tmp2 := make([]byte, 8)

	if withKey {
		// The `withKey` option should only be true when dumping the vkv entry in a meta blob

		// Store the key length
		binary.BigEndian.PutUint32(tmp[:], uint32(len(ve.Key)))
		if _, err := buf.Write(tmp); err != nil {
			return nil, err
		}

		// Store the actual key
		if _, err := buf.WriteString(ve.Key); err != nil {
			return nil, err
		}
	}

	// Store the version
	binary.BigEndian.PutUint64(tmp2[:], uint64(ve.Version))
	if _, err := buf.Write(tmp2); err != nil {
		return nil, err
	}

	// Store the Hash/Data
	switch ve.Flag {
	case HashOnly, HashAndData:
		bhash, err := hex.DecodeString(ve.Hash)
		if err != nil {
			return nil, err
		}
		if _, err := buf.Write(bhash); err != nil {
			return nil, err
		}
		if ve.Flag == HashOnly {
			return buf.Bytes(), nil
		}

		// Execute DataOnly too since the flag in HashAndData
		fallthrough
	case DataOnly:
		if _, err := buf.Write(ve.Data); err != nil {
			return nil, err
		}

	}
	return buf.Bytes(), nil
}

func UnserializeBlob(data []byte) (*KeyValue, error) {
	r := bytes.NewReader(data)
	tmp := make([]byte, 4)
	tmp2 := make([]byte, 8)

	// TODO(tsileo): assert the version flag is 0
	if _, err := r.ReadByte(); err != nil {
		return nil, err
	}

	// Read the first flag
	flag, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	// Read the Key size
	if _, err := r.Read(tmp); err != nil {
		return nil, err
	}
	keySize := int(binary.BigEndian.Uint32(tmp[:]))

	// Read the key (now that we know the size)
	bkey := make([]byte, keySize)
	if _, err := r.Read(bkey); err != nil {
		return nil, err
	}

	// Read the version
	if _, err := r.Read(tmp2); err != nil {
		return nil, err
	}
	version := int(binary.BigEndian.Uint64(tmp2[:]))

	ve := &KeyValue{
		Key:     string(bkey),
		Version: version,
		Flag:    Flag(flag),
	}

	// Read the Hash/Data according to the set Flag
	switch ve.Flag {
	case HashOnly, HashAndData:
		bhash := make([]byte, 32)
		if _, err := r.Read(bhash); err != nil {
			return nil, err
		}
		ve.Hash = hex.EncodeToString(bhash)
		if ve.Flag == HashOnly {
			return ve, nil
		}
		fallthrough
	case DataOnly:
		// if _, err := r.Read(tmp); err != nil {
		// 	return nil, err
		// }
		// size := int(binary.BigEndian.Uint32(tmp[:]))
		size := len(data) - (14 + keySize)
		if ve.Flag == HashAndData {
			size -= 32
		}
		data := make([]byte, size)
		if _, err := r.Read(data); err != nil {
			return nil, err
		}
		ve.Data = data
	}

	return ve, nil
}

func Unserialize(key string, data []byte) (*KeyValue, error) {
	r := bytes.NewReader(data)
	tmp2 := make([]byte, 8)

	// TODO(tsileo): assert the version flag is 0
	if _, err := r.ReadByte(); err != nil {
		return nil, err
	}

	// Read the first flag
	flag, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	// Read the version
	if _, err := r.Read(tmp2); err != nil {
		return nil, err
	}
	version := int(binary.BigEndian.Uint64(tmp2[:]))

	ve := &KeyValue{
		Key:     key,
		Version: version,
		Flag:    Flag(flag),
	}

	// Read the Hash/Data according to the set Flag
	switch ve.Flag {
	case HashOnly, HashAndData:
		bhash := make([]byte, 32)
		if _, err := r.Read(bhash); err != nil {
			return nil, err
		}
		ve.Hash = hex.EncodeToString(bhash)
		if ve.Flag == HashOnly {
			return ve, nil
		}
		fallthrough
	case DataOnly:
		size := len(data) - 10
		if ve.Flag == HashAndData {
			size -= 32
		}
		if size > 0 {
			data := make([]byte, size)
			if _, err := r.Read(data); err != nil {
				return nil, err
			}
			ve.Data = data
		}
	}

	return ve, nil
}
