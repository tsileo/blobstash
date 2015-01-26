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

package vkv

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync"
	"time"

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

var ErrNotFound = errors.New("vkv: key does not exist")

// KeyValue holds a singke key value pair, along with the version (the creation timestamp)
type KeyValue struct {
	Key     string `json:"key,omitempty"`
	Value   string `json:"value"`
	Version int    `json:"version"`
	db      *DB
}

func (kvi *KeyValue) SetMetaBlob(hash string) error {
	return kvi.db.setmetablob(kvi.Key, kvi.Version, hash)
}

// KeyValueVersions holds the full history for a key value pair
type KeyValueVersions struct {
	Key      string      `json:"key"`
	Versions []*KeyValue `json:"versions"`
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

// Store a uint64 as binary data.
func (db *DB) putUint64(key []byte, value uint64) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	val := make([]byte, 8)
	binary.LittleEndian.PutUint64(val[:], value)
	err := db.db.Set(key, val)
	return err
}

// Retrieve a binary stored uint64.
func (db *DB) getUint64(key []byte) (uint64, error) {
	data, err := db.db.Get(nil, key)
	if err != nil || data == nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(data), nil
}

// Increment a binary stored uint64.
func (db *DB) incrUint64(key []byte, step int) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	data, err := db.db.Get(nil, key)
	var value uint64
	if err != nil {
		return err
	}
	if data == nil {
		value = 0
	} else {
		value = binary.LittleEndian.Uint64(data)
	}
	val := make([]byte, 8)
	binary.LittleEndian.PutUint64(val[:], value+uint64(step))
	err = db.db.Set(key, val)
	return err
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

// Get the length of the list
func (db *DB) VersionCnt(key string) (int, error) {
	bkey := []byte(key)
	cardkey := encodeMeta(KvVersionCnt, bkey)
	card, err := db.getUint64(encodeMeta(Meta, cardkey))
	return int(card), err
}

func (db *DB) DeleteVersion(key string, version int) error {
	// TODO
	return nil
}

// MetaBlob retrns the meta blob where this key/version is stored
func (db *DB) MetaBlob(key string, version int) (string, error) {
	bhash, err := db.db.Get(nil, encodeMeta(KvItemMeta, encodeKey([]byte(key), version)))
	if err != nil {
		return "", err
	}
	return string(bhash), nil
}

func (db *DB) setmetablob(key string, version int, hash string) error {
	return db.db.Set(encodeMeta(KvItemMeta, encodeKey([]byte(key), version)), []byte(hash))
}

// Put updates the value for the given version associated with key,
// if version == -1, version will be set to time.Now().UTC().UnixNano().
func (db *DB) Put(key, value string, version int) (*KeyValue, error) {
	if version == -1 {
		version = int(time.Now().UTC().UnixNano())
	}
	bkey := []byte(key)
	cmin, err := db.getUint64(encodeMeta(KvVersionMin, bkey))
	if err != nil {
		return nil, err
	}
	cmax, err := db.getUint64(encodeMeta(KvVersionMax, bkey))
	if err != nil {
		return nil, err
	}
	llen := -1
	if cmin == 0 && cmax == 0 {
		llen, err = db.VersionCnt(key)
		if err != nil {
			return nil, err
		}
	}
	if llen == 0 || int(cmin) > version {
		if err := db.putUint64(encodeMeta(KvVersionMin, bkey), uint64(version)); err != nil {
			return nil, err
		}
	}
	if cmax == 0 || int(cmax) < version {
		if err := db.putUint64(encodeMeta(KvVersionMax, bkey), uint64(version)); err != nil {
			return nil, err
		}
	}
	kmember := encodeKey(bkey, version)
	cval, err := db.db.Get(nil, kmember)
	if err != nil {
		return nil, err
	}
	// TODO prevent overwriting an existing version
	if cval == nil {
		cardkey := encodeMeta(KvVersionCnt, bkey)
		if err := db.incrUint64(encodeMeta(Meta, cardkey), 1); err != nil {
			return nil, err
		}
	}
	if err := db.db.Set(kmember, []byte(value)); err != nil {
		return nil, err
	}
	if err := db.db.Set(encodeMeta(KvKeyIndex, bkey), []byte{1}); err != nil {
		return nil, err
	}
	return &KeyValue{
		Key:     key,
		Value:   value,
		Version: version,
		db:      db,
	}, nil
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
// if version == -1, the latest version will be returned.
func (db *DB) Get(key string, version int) (*KeyValue, error) {
	bkey := []byte(key)
	exists, err := db.db.Get(nil, encodeMeta(KvKeyIndex, bkey))
	if err != nil {
		return nil, err
	}
	if len(exists) == 0 {
		return nil, ErrNotFound
	}
	if version == -1 {
		max, err := db.getUint64(encodeMeta(KvVersionMax, bkey))
		if err != nil {
			return nil, err
		}
		version = int(max)
	}
	val, err := db.db.Get(nil, encodeKey(bkey, version))
	if err != nil {
		return nil, err
	}
	return &KeyValue{
		Key:     key,
		Version: version,
		Value:   string(val),
	}, nil
}

// Return a lexicographical range
func (db *DB) Versions(key string, start, end, limit int) (*KeyValueVersions, error) {
	res := &KeyValueVersions{
		Key:      key,
		Versions: []*KeyValue{},
	}
	bkey := []byte(key)
	exists, err := db.db.Get(nil, encodeMeta(KvKeyIndex, bkey))
	if err != nil {
		return nil, err
	}
	if len(exists) == 0 {
		return nil, ErrNotFound
	}
	enum, _, err := db.db.Seek(encodeKey(bkey, start))
	if err != nil {
		return nil, err
	}
	endBytes := encodeKey(bkey, end)
	i := 0
	for {
		k, v, err := enum.Next()
		if err == io.EOF {
			break
		}
		if bytes.Compare(k, endBytes) > 0 || (limit != 0 && i > limit) {
			return res, nil
		}
		_, index := decodeKey(k)
		res.Versions = append(res.Versions, &KeyValue{
			Value:   string(v),
			Version: index,
		})
		i++
	}
	return res, nil
}

// Return a lexicographical range
func (db *DB) Keys(start, end string, limit int) ([]*KeyValue, error) {
	res := []*KeyValue{}
	enum, _, err := db.db.Seek(encodeMeta(KvKeyIndex, []byte(start)))
	if err != nil {
		return nil, err
	}
	endBytes := encodeMeta(KvKeyIndex, []byte(end))
	i := 0
	for {
		k, _, err := enum.Next()
		if err == io.EOF {
			break
		}
		if bytes.Compare(k, endBytes) > 0 || (limit != 0 && i > limit) {
			return res, nil
		}
		kv, err := db.Get(string(k[1:]), -1)
		if err != nil {
			return nil, err
		}
		res = append(res, kv)
		i++
	}
	return res, nil
}
