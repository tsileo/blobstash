package db

import (
	"bytes"
	"encoding/binary"
	_ "fmt"
	"github.com/jmhodges/levigo"
	"strconv"
	"sync"
)

//
// ## Strings
//
// String key are stored this way:
//   String byte + key => value
// and a key holds the number of string key:
//   Meta + StringCnt => binary encoded uint32
//

const (
	Meta byte = iota
	MetaTTL
	String
	StringCnt
	Set
	SetCardinality
	SetCnt
	List
	ListLen
	ListCnt
	Hash
	HashFieldsCnt
	HashCnt
	BlobsCnt
	BlobsSize
)

const SnapshotTTL = 120

// Format a key (append the "type" byte)
func KeyType(key interface{}, kType byte) []byte {
	var keybyte []byte
	switch k := key.(type) {
	case []byte:
		keybyte = k
	case string:
		keybyte = []byte(k)
	case byte:
		keybyte = []byte{k}
	}
	k := make([]byte, 1+len(keybyte))
	k[0] = kType
	copy(k[1:], keybyte)
	return k
}

type KeyValue struct {
	Key   string
	Value string
}

// Perform a lexico range query
func GetRange(db *levigo.DB, ro *levigo.ReadOptions, kStart []byte, kEnd []byte, limit int) (values []*KeyValue, err error) {
	it := db.NewIterator(ro)
	defer func() {
		it.Close()
	}()
	it.Seek(kStart)
	endBytes := kEnd

	i := 0
	for {
		if it.Valid() {
			if bytes.Compare(it.Key(), endBytes) > 0 || (limit != 0 && i > limit) {
				return
			}
			value := it.Value()
			vstr := string(value[:])
			key := it.Key()
			// Drop the meta byte
			kstr := string(key[1:])
			values = append(values, &KeyValue{kstr, vstr})
			it.Next()
			i++
		} else {
			err = it.GetError()
			return
		}
	}

	return
}

// The key-value database.
type DB struct {
	ldb          *levigo.DB
	ldb_path string
	mutex        *SlottedMutex
	wo           *levigo.WriteOptions
	ro           *levigo.ReadOptions
	snapMutex    *sync.Mutex
	snapshots    map[string]*levigo.Snapshot
	snapshotsTTL map[string]int64
}

// Creates a new database.
func New(ldb_path string) (*DB, error) {
	opts := levigo.NewOptions()
	opts.SetCreateIfMissing(true)
	filter := levigo.NewBloomFilter(10)
	opts.SetFilterPolicy(filter)
	db, err := levigo.Open(ldb_path, opts)
	mutex := NewSlottedMutex()
	return &DB{ldb: db, ldb_path: ldb_path, mutex: mutex,
		wo: levigo.NewWriteOptions(), ro: levigo.NewReadOptions(),
		snapMutex: &sync.Mutex{}, snapshots: map[string]*levigo.Snapshot{}, snapshotsTTL: map[string]int64{}}, err
}

func (db *DB) Destroy() error {
	opts := levigo.NewOptions()
	err := levigo.DestroyDatabase(db.ldb_path, opts)
	return err
}

// Cleanly close the DB
func (db *DB) Close() {
	db.wo.Close()
	db.ro.Close()
	db.ldb.Close()
}

// Retrieves the value for a given key.
func (db *DB) get(key []byte) ([]byte, error) {
	db.mutex.Lock([]byte(key))
	defer db.mutex.Unlock([]byte(key))
	data, err := db.ldb.Get(db.ro, key)
	return data, err
}

func (db *DB) getset(key []byte, value []byte) ([]byte, error) {
	db.mutex.Lock(key)
	defer db.mutex.Unlock(key)
	cval, err := db.ldb.Get(db.ro, key)
	if err != nil {
		return cval, err
	}
	err = db.ldb.Put(db.wo, key, value)
	return cval, err
}

// Sets the value for a given key.
func (db *DB) put(key []byte, value []byte) error {
	db.mutex.Lock(key)
	defer db.mutex.Unlock(key)
	err := db.ldb.Put(db.wo, key, value)
	return err
}

// Delete the key
func (db *DB) del(key []byte) error {
	db.mutex.Lock(key)
	defer db.mutex.Unlock(key)
	err := db.ldb.Delete(db.wo, key)
	return err
}

// Store a uint32 as binary data
func (db *DB) putUint32(key []byte, value uint32) error {
	db.mutex.Lock(key)
	defer db.mutex.Unlock(key)
	val := make([]byte, 4)
	binary.LittleEndian.PutUint32(val[:], value)
	err := db.ldb.Put(db.wo, key, val)
	return err
}

// Retrieve a binary stored uint32
func (db *DB) getUint32(key []byte) (uint32, error) {
	db.mutex.Lock(key)
	defer db.mutex.Unlock(key)
	data, err := db.ldb.Get(db.ro, key)
	if err != nil || data == nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(data), nil
}

// Increment a binary stored uint32
func (db *DB) incrUint32(key []byte, step int) error {
	db.mutex.Lock(key)
	defer db.mutex.Unlock(key)
	data, err := db.ldb.Get(db.ro, key)
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
	err = db.ldb.Put(db.wo, key, val)
	return err
}

// Increment the given string key, the key is created is it doesn't exists
func (db *DB) incrby(key []byte, value int) error {
	db.mutex.Lock(key)
	defer db.mutex.Unlock(key)
	sval, err := db.ldb.Get(db.ro, key)
	if err != nil {
		return err
	}
	if sval == nil {
		sval = []byte("0")
		err = db.incrUint32(KeyType(StringCnt, Meta), 1)
		if err != nil {
			return err
		}
	}
	ival, err := strconv.Atoi(string(sval))
	if err != nil {
		return err
	}
	err = db.ldb.Put(db.wo, key, []byte(strconv.Itoa(ival+value)))
	return err
}
