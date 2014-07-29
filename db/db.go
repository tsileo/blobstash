/*

Package db implements a "data structure" database built on top of kv [1]: strings, hashes, sorted list, and sets.

Links

	[1] https://github.com/cznic/kv

*/
package db

import (
	"bytes"
	"encoding/binary"
	"github.com/cznic/kv"
	"io"
	"os"
	"strconv"
)

//
// ## Strings
//
// String key are stored this way:
//   String byte + key => value
//

// Define namespaces for raw key sorted in db.
const (
	Empty byte = iota
	Meta
	String
	Set
	SetCardinality
	List
	ListLen
	ListMin // List boundaries for seeking
	ListMax
	Hash
	HashFieldsCnt
	HashIndex
)

func opts() *kv.Options {
	return &kv.Options{
		VerifyDbBeforeOpen:  true,
		VerifyDbAfterOpen:   true,
		VerifyDbBeforeClose: true,
		VerifyDbAfterClose:  true,
	}
}

// Format a key (append the given "type" byte).
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
	k := make([]byte, len(keybyte)+1)
	k[0] = kType
	copy(k[1:], keybyte)
	return k
}

// KeyValue represents a key-value pair, also used to represents hashes attributes.
type KeyValue struct {
	Key   string
	Value string
}

// IndexValue represents a sorted list index-value pair.
type IndexValue struct {
	Index int
	Value string
}

// GetRange performs a lexical range query.
func GetRange(db *kv.DB, kStart []byte, kEnd []byte, limit int) (values []*KeyValue, err error) {
	enum, _, err := db.Seek(kStart)
	endBytes := kEnd
	i := 0
	for {
		k, v, err := enum.Next()
		if err == io.EOF {
			break
		}
		if bytes.Compare(k, endBytes) > 0 || (limit != 0 && i > limit) {
			return values, nil
		}
		vstr := string(v)
		kstr := string(k[1:])
		values = append(values, &KeyValue{kstr, vstr})
		i++
	}
	return
}

func GetReverseRange(db *kv.DB, kStart []byte, kEnd []byte, limit int) (values []*KeyValue, err error) {
	enum, _, err := db.Seek(kStart)
	endBytes := kEnd
	i := 0
	for {
		k, v, err := enum.Prev()
		if err == io.EOF {
			break
		}
		if bytes.Compare(k, endBytes) < 0 || (limit != 0 && i > limit) {
			return values, nil
		}
		vstr := string(v)
		kstr := string(k[1:])
		values = append(values, &KeyValue{kstr, vstr})
		i++
	}
	return
}

// GetRangeLast performs a lexical range query, and return the last key-value pair.
func GetListLastRange(db *kv.DB, kStart []byte) (kv *KeyValue, err error) {
	enum, _, err := db.Seek(kStart)
	enum.Prev()
	k, v, err := enum.Prev()
	if err == io.EOF {
		return
	}
	vstr := string(v)
	kstr := string(k[1:])
	kv = &KeyValue{kstr, vstr}
	return
}
// GetRangeLast performs a lexical range query, and return the last key-value pair.
func GetRangeLast(db *kv.DB, kStart []byte, kEnd []byte, limit int) (kv *KeyValue, err error) {
	enum, _, err := db.Seek(kStart)
	endBytes := kEnd
	i := 0
	for {
		k, v, err := enum.Next()
		if err == io.EOF {
			break
		}
		if bytes.Compare(k, endBytes) > 0 || (limit != 0 && i > limit) {
			return kv, nil
		}
		vstr := string(v)
		kstr := string(k[1:])
		kv = &KeyValue{kstr, vstr}
		i++
	}
	return
}

// GetMinRange perform a lexico range query but try to return a least two values,
// even if  if the key is not "greater than or equal to" the given key.
// For list, the list name will be checked later on.
func GetMinRange(db *kv.DB, kStart []byte, kEnd []byte, limit int) (values []*KeyValue, err error) {
	enum, _, err := db.Seek(kStart)
	endBytes := kEnd
	i := 0
	for {
		k, v, err := enum.Next()
		if err == io.EOF {
			break
		}
		if (bytes.Compare(k, endBytes) > 0 && len(values) >= 2) || (limit != 0 && i > limit) {
			return values, nil
		}
		vstr := string(v)
		kstr := string(k[1:])
		values = append(values, &KeyValue{kstr, vstr})
		i++
	}
	return
}

// The key-value database.
type DB struct {
	db      *kv.DB
	db_path string
	mutex   *SlottedMutex
}

type DBReader interface {
	Get(key string) (val []byte, err error)
}

// New creates a new database.
func New(db_path string) (*DB, error) {
	createOpen := kv.Open
	if _, err := os.Stat(db_path); os.IsNotExist(err) {
		createOpen = kv.Create
	}
	//opts := opts()
	//action := kv.Open
	//if _, err := os.Stat(db_path); os.IsNotExist(err) {
	//	action = kv.Create
	//}
	kvdb, err := createOpen(db_path, &kv.Options{})
	db := &DB{}
	db.db = kvdb
	db.db_path = db_path
	db.mutex = NewSlottedMutex()
	return db, err
}

// NewMem initialize a in-memory database (used for testing purpose).
//func NewMem() (*DB, error) {
//	db, err := kv.CreateMem(&kv.Options{})
//	mutex := NewSlottedMutex()
//	return &DB{db: db, db_path: "", mutex: mutex}, err
//}

// Destroy remove completely the DB.
func (db *DB) Destroy() error {
	if db.db_path != "" {
		db.Close()
		return os.RemoveAll(db.db_path)
	}
	return nil
}

// Cleanly close the DB.
func (db *DB) Close() {
	db.db.Close()
}

// BeginTransaction is a shortcut to call cznic/kv BeginTransaction.
// (it starts a new transaction).
func (db *DB) BeginTransaction() (err error) {
	return db.db.BeginTransaction()
}

// Commit is a shortcut to call cznic/kv Commit
// (it try commits the current transaction).
func (db *DB) Commit() (err error) {
	return db.db.Commit()
}

// Rollback is a shortcut to call cznic/kv Rollback
// (it cancels and undoes the current transaction).
func (db *DB) Rollback() (err error) {
	return db.db.Rollback()
}

// Retrieves the value for a given key.
func (db *DB) get(key []byte) ([]byte, error) {
	db.mutex.Lock([]byte(key))
	defer db.mutex.Unlock([]byte(key))
	data, err := db.db.Get(nil, key)
	if data != nil && data[0] == Empty {
		data = []byte{}
	}
	return data, err
}

// put sets the value for a given key.
func (db *DB) put(key, value []byte) error {
	db.mutex.Lock(key)
	defer db.mutex.Unlock(key)
	if len(value) == 0 {
		value = []byte{Empty}
	}
	err := db.db.Set(key, value)
	return err
}

// del delete the given key.
func (db *DB) del(key []byte) error {
	db.mutex.Lock(key)
	defer db.mutex.Unlock(key)
	err := db.db.Delete(key)
	return err
}

// Store a uint32 as binary data.
func (db *DB) putUint32(key []byte, value uint32) error {
	db.mutex.Lock(key)
	defer db.mutex.Unlock(key)
	val := make([]byte, 4)
	binary.LittleEndian.PutUint32(val[:], value)
	err := db.db.Set(key, val)
	return err
}

// Retrieve a binary stored uint32.
func (db *DB) getUint32(key []byte) (uint32, error) {
	db.mutex.Lock(key)
	defer db.mutex.Unlock(key)
	data, err := db.db.Get(nil, key)
	if err != nil || data == nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(data), nil
}

// Increment a binary stored uint32.
func (db *DB) incrUint32(key []byte, step int) error {
	db.mutex.Lock(key)
	defer db.mutex.Unlock(key)
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

// Increment the given string key, the key is created is it doesn't exists.
func (db *DB) incrby(key []byte, value int) error {
	db.mutex.Lock(key)
	defer db.mutex.Unlock(key)
	sval, err := db.db.Get(nil, key)
	if err != nil {
		return err
	}
	var ival int
	if string(sval) != "" {
		iival, err := strconv.Atoi(string(sval))
		if err != nil {
			return err
		}
		ival = iival
	}
	err = db.db.Set(key, []byte(strconv.Itoa(ival+value)))
	return err
}
