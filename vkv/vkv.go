/*

Package vkv implements a versioned key value store.

The change history for all keys are kept and versioned by timestamp.

Keys are sorted lexicographically.

*/

package vkv

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"time"

	"github.com/cznic/kv"
)

// Define namespaces for raw key sorted in db.
const (
	Empty byte = iota
	Meta
	KvItem
	KvVersionCnt
	kvVersionMin
	KvVersionMax
)

// KeyValue holds a singke key value pair, along with the version (the creation timestamp)
type KeyValue struct {
	Key     string `json:"key,omitempty"`
	Value   string `json:"value"`
	Version int    `json:"version"`
}

// KeyValueVersions holds the full history for a key value pair
type KeyValueVersions struct {
	Key      string      `json:"key"`
	Versions []*KeyValue `json:"versions"`
}

type DB struct {
	db   *kv.DB
	path string
}

// New creates a new database.
func New(db_path string) (*DB, error) {
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

// Store a uint32 as binary data.
func (db *DB) putUint32(key []byte, value uint32) error {
	val := make([]byte, 4)
	binary.LittleEndian.PutUint32(val[:], value)
	err := db.db.Set(key, val)
	return err
}

// Retrieve a binary stored uint32.
func (db *DB) getUint32(key []byte) (uint32, error) {
	data, err := db.db.Get(nil, key)
	if err != nil || data == nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(data), nil
}

// Increment a binary stored uint32.
func (db *DB) incrUint32(key []byte, step int) error {
	data, err := db.db.Get(nil, key)
	var value uint32
	if err != nil {
		return err
	}
	if data == nil {
		value = 0
	} else {
		value = binary.LittleEndian.Uint32(data)
	}
	val := make([]byte, 4)
	binary.LittleEndian.PutUint32(val[:], value+uint32(step))
	err = db.db.Set(key, val)
	return err
}

func encodeKey(key []byte, index int) []byte {
	indexbyte := make([]byte, 4)
	binary.BigEndian.PutUint32(indexbyte, uint32(index))
	k := make([]byte, len(key)+9)
	k[0] = List
	binary.LittleEndian.PutUint32(k[1:5], uint32(len(key)))
	copy(k[5:], key)
	copy(k[5+len(key):], indexbyte)
	return k
}

// Extract the index from the raw key
func decodeKey(key []byte) (string, int) {
	klen := int(binary.LittleEndian.Uint32(key[1:5]))
	index := int(binary.BigEndian.Uint32(key[len(key)-4:]))
	member := make([]byte, klen)
	copy(member, key[5:5+klen])
	return member, int
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
	card, err := db.getUint32(encodeMeta(Meta, cardkey))
	return int(card), err
}

func (db *DB) Set(key, value string) (*KeyValue, error) {
	index := int(time.Now().UTC().Unix())
	bkey := []byte(key)
	cmin, err := db.getUint32(encodeMeta(KvVersionMin, bkey))
	if err != nil {
		return nil, err
	}
	cmax, err := db.getUint32(encodeMeta(KvVersionMax, bkey))
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
	if llen == 0 || int(cmin) > index {
		if err := db.putUint32(encodeMeta(KvVersionMin, bkey), uint32(index)); err != nil {
			return nil, err
		}
	}
	if cmax == 0 || int(cmax) < index {
		if err := db.putUint32(encodeMeta(KvVersionMax, bkey), uint32(index)); err != nil {
			return nil, err
		}
	}
	kmember := encodeKey(bkey, index)
	cval, err := db.db.Get(kmember)
	if err != nil {
		return nil, err
	}
	if err := db.db.Put(kmember, []byte(value)); err != nil {
		return nil, err
	}
	if cval == nil {
		cardkey := encodeMeta(KvVersionCnt, bkey)
		if err := db.incrUint32(encodeMeta(Meta, cardkey), 1); err != nil {
			return nil, err
		}
	}
	return &KeyValue{
		Key:   key,
		Value: value,
		Index: index,
	}, nil
}

// Get returns the latest value for the given key
func (db *DB) Get(key string) (*KeyValue, error) {
	bkey := []byte(key)
	max, err := db.getUint32(encodeMeta(KvVersionMax, bkey))
	if err != nil {
		return nil, err
	}
	val, err := db.db.Get(encodeKey(bkey, int(max)))
	if err != nil {
		return nil, err
	}
	return &KeyValue{
		Key:   key,
		Index: int(max),
		Value: string(val),
	}, nil
}

// Return a lexicographical range
func (db *DB) Versions(key string, start, end, limit int) (*KeyValueVersions, error) {
	res := &KeyValueVersions{
		Key:      key,
		Versions: []*KeyValue{},
	}
	bkey := []byte(key)
	enum, _, err := db.db.Seek(encodeKey(bkey, start))
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
		index, _ := decodeKey(k)
		res.Versions = append(res.Versions, &KeyValue{
			Value: string(v),
			Index: index,
		})
		i++
	}
	return res
}

// TODO Keys(start, end string) ([]string, error)
//	Add the key index meta
//	Add tests
