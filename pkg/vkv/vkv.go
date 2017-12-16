package vkv // import "a4.io/blobstash/pkg/vkv"

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
	"time"

	"github.com/vmihailenco/msgpack"

	"a4.io/blobstash/pkg/rangedb"
)

const schemaVersion = 1

const (
	Sep              = ':'
	FlagUnknown byte = iota
	FlagMetaBlob
	FlagVersion
	FlagKey
)

// KvType for meta serialization
const KvType = "kv"

var ErrNotFound = errors.New("vkv: key not found")

type KeyValue struct {
	SchemaVersion int `msgpack:"_v"`

	Key     string `msgpack:"k,omitempty"`
	Version int    `msgpack:"v"`
	Hash    []byte `msgpack:"h,omitempty"`
	Data    []byte `msgpack:"d,omitempty"`
}

// Implements the `MetaData` interface
func (kv *KeyValue) Type() string {
	return KvType
}

// Implements the `MetaData` interface
func (kv *KeyValue) Dump() ([]byte, error) {
	kv.SchemaVersion = schemaVersion
	return msgpack.Marshal(kv)
}

func (kv *KeyValue) SetHexHash(h string) error {
	hash, err := hex.DecodeString(h)
	if err != nil {
		return err
	}
	kv.Hash = hash
	return nil
}

func (kv *KeyValue) HexHash() string {
	if kv.Hash != nil && len(kv.Hash) > 0 {
		return hex.EncodeToString(kv.Hash)
	}
	return ""
}

// KeyValueVersions holds the full history for a key value pair
type KeyValueVersions struct {
	Key string `json:"key"`

	// FIXME(tsileo): turn this into a []*VkvEntry
	Versions []*KeyValue `json:"versions"`
}

func NextVersionCursor(key string) string {
	v, err := strconv.Atoi(key)
	if err != nil {
		panic(fmt.Errorf("should never happen, key=%s", key))
	}
	return strconv.Itoa(v - 1)
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
	rdb *rangedb.RangeDB
	mu  sync.Mutex
}

// New creates a new database.
func New(path string) (*DB, error) {
	rdb, err := rangedb.New(path)
	if err != nil {
		return nil, err
	}
	return &DB{rdb: rdb}, nil
}

func (db *DB) Close() error { return db.rdb.Close() }

func (db *DB) Destroy() error { return db.rdb.Destroy() }

func (db *DB) Get(key string, version int) (*KeyValue, error) {
	if version <= 0 {
		return db.get(key)
	}
	return db.getAt(key, version)
}

func (db *DB) get(key string) (*KeyValue, error) {
	kvkey := append([]byte{FlagKey}, []byte(key)...)
	data, err := db.rdb.Get(kvkey)
	if err != nil {
		return nil, err
	}
	if data == nil || len(data) == 0 {
		return nil, ErrNotFound
	}

	res := &KeyValue{Key: key}
	if err := msgpack.Unmarshal(data, res); err != nil {
		return nil, err
	}

	return res, nil
}

func (db *DB) Put(kv *KeyValue) error {
	// db.mu.Lock()
	// defer db.mu.Unlock()
	kv.SchemaVersion = schemaVersion

	if kv.Version < 1 {
		kv.Version = int(time.Now().UTC().UnixNano())
	}

	encoded, err := kv.Dump()
	if err != nil {
		return err
	}

	// Set the regular key
	kvkey := append([]byte{FlagKey}, []byte(kv.Key)...)

	// But only if it's the latest version (or there's no previous version)
	ckv, err := db.get(kv.Key)
	if err != nil && err != ErrNotFound {
		return err
	}

	if ckv == nil || kv.Version > ckv.Version {
		if err := db.rdb.Set(kvkey, encoded); err != nil {
			return err
		}
	}

	// Set the version key (for keeping track of all the versions)
	vkey := buildVkey(kvkey, kv.Version)
	if err := db.rdb.Set(vkey, encoded); err != nil {
		return err
	}

	return nil
}

func buildVkey(kvkey []byte, version int) []byte {
	klen := len(kvkey) - 1
	vkey := make([]byte, klen+10)

	// Set the version flag
	vkey[0] = FlagVersion

	// Copy the key
	copy(vkey[1:], kvkey[1:])

	// Add separator
	vkey[klen+1] = Sep

	// Add the binary encoded version
	binary.BigEndian.PutUint64(vkey[klen+2:], uint64(version))

	return vkey
}

func buildMetaBlobKey(key []byte, version int) []byte {
	klen := len(key)
	vkey := make([]byte, klen+10)

	// Set the version flag
	vkey[0] = FlagMetaBlob

	// Copy the key
	copy(vkey[1:], key[:])

	// Add separator
	vkey[klen+1] = Sep

	// Add the binary encoded version
	binary.BigEndian.PutUint64(vkey[klen+2:], uint64(version))

	return vkey
}

func (db *DB) SetMetaBlob(key string, version int, hash string) error {
	vkey := buildMetaBlobKey([]byte(key), version)

	h, err := hex.DecodeString(hash)
	if err != nil {
		return err
	}

	if err := db.rdb.Set(vkey, h); err != nil {
		return err
	}

	return nil
}

func (db *DB) GetMetaBlob(key string, version int) (string, error) {
	if version <= 0 {
		kv, err := db.get(key)
		if err != nil {
			return "", err
		}
		version = kv.Version
	}

	vkey := buildMetaBlobKey([]byte(key), version)

	data, err := db.rdb.Get(vkey)
	if err != nil {
		return "", err
	}

	if data != nil && len(data) > 0 {
		return hex.EncodeToString(data), nil
	}

	return "", nil
}

func (db *DB) getAt(key string, version int) (*KeyValue, error) {
	kvkey := append([]byte{FlagKey}, []byte(key)...)
	vkey := buildVkey(kvkey, version)
	data, err := db.rdb.Get(vkey)
	if err != nil {
		return nil, err
	}
	if data == nil || len(data) == 0 {
		return nil, ErrNotFound
	}

	res := &KeyValue{Key: key}
	if err := msgpack.Unmarshal(data, res); err != nil {
		return nil, err
	}

	return res, nil
}

func (db *DB) keys(start, end string, limit int, reverse bool) ([]*KeyValue, string, error) {
	var cursor string
	out := []*KeyValue{}

	c := db.rdb.Range(append([]byte{FlagKey}, []byte(start)...), append([]byte{FlagKey}, []byte(end)...), reverse)

	// Iterate the range
	k, v, err := c.Next()
	for ; err == nil && (limit <= 0 || len(out) < limit); k, v, err = c.Next() {
		res := &KeyValue{Key: string(k[1:])}
		if err := msgpack.Unmarshal(v, res); err != nil {
			return nil, cursor, err
		}

		out = append(out, res)
	}

	if len(out) > 0 {
		// Generate next cursor
		rcursor := out[len(out)-1].Key
		if reverse {
			cursor = PrevKey(rcursor)
		} else {
			cursor = NextKey(rcursor)
		}
	}

	// Return
	if err == io.EOF {
		return out, cursor, nil
	}

	return out, cursor, nil

}

func (db *DB) Keys(start, end string, limit int) ([]*KeyValue, string, error) {
	return db.keys(start, end, limit, false)
}

func (db *DB) ReverseKeys(start, end string, limit int) ([]*KeyValue, string, error) {
	return db.keys(start, end, limit, true)
}

func (db *DB) Versions(key string, start, end, limit int) (*KeyValueVersions, int, error) {
	var nstart int
	res := &KeyValueVersions{
		Key:      key,
		Versions: []*KeyValue{},
	}
	if end <= 0 {
		end = int(time.Now().UTC().UnixNano())
	}

	kvkey := append([]byte{FlagKey}, []byte(key)...)
	rstart := buildVkey(kvkey, start)
	rend := buildVkey(kvkey, end)
	c := db.rdb.Range(rstart, rend, true)

	// Iterate the range
	_, v, err := c.Next()
	for ; err == nil && (limit <= 0 || len(res.Versions) < limit); _, v, err = c.Next() {
		kv := &KeyValue{Key: key}
		if err := msgpack.Unmarshal(v, kv); err != nil {
			return nil, nstart, err
		}

		res.Versions = append(res.Versions, kv)

		nstart = kv.Version - 1
	}

	return res, nstart, nil
}

func UnserializeBlob(blob []byte) (*KeyValue, error) {
	kv := &KeyValue{}
	if err := msgpack.Unmarshal(blob, kv); err != nil {
		return nil, err
	}
	return kv, nil
}
