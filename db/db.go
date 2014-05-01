package db

import (
	"bytes"
	"encoding/binary"
	_ "fmt"
	"github.com/cznic/kv"
	"strconv"
	"io"
	"os"
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
	Empty byte = iota
	Meta
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
	HashIndex
	HashCnt
	BlobsCnt
	BlobsSize
)

func opts() *kv.Options {
	return &kv.Options{
		VerifyDbBeforeOpen:  true,
		VerifyDbAfterOpen:   true,
		VerifyDbBeforeClose: true,
		VerifyDbAfterClose:  true,
	}
}

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

type IndexValue struct {
	Index int
	Value string
}

// Perform a lexico range query
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

// Perform a lexico range query
func GetRangeWithPrev(db *kv.DB, kStart []byte, kEnd []byte, limit int) (values []*KeyValue, err error) {
	enum, _, err := db.Seek(kStart)
	enum.Prev()
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

// The key-value database.
type DB struct {
	db          *kv.DB
	db_path string
	mutex        *SlottedMutex
}

// Creates a new database.
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
	db, err := createOpen(db_path, &kv.Options{})
	mutex := NewSlottedMutex()
	return &DB{db: db, db_path: db_path, mutex: mutex}, err
}

func NewMem() (*DB, error) {
	db, err := kv.CreateMem(&kv.Options{})
	mutex := NewSlottedMutex()
	return &DB{db: db, db_path: "", mutex: mutex}, err
}

func (db *DB) Destroy() error {
	if db.db_path != "" {
		return os.RemoveAll(db.db_path)
	}
	return nil
}

// Cleanly close the DB
func (db *DB) Close() {
	db.db.Close()
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

func (db *DB) getset(key, value []byte) ([]byte, error) {
	db.mutex.Lock(key)
	defer db.mutex.Unlock(key)
	cval, err := db.db.Get(nil, key)
	if err != nil {
		return cval, err
	}
	err = db.db.Set(key, value)
	return cval, err
}

// Sets the value for a given key.
func (db *DB) put(key []byte, value []byte) error {
	db.mutex.Lock(key)
	defer db.mutex.Unlock(key)
	if len(value) == 0 {
		value = []byte{Empty}
	}
	err := db.db.Set(key, value)
	return err
}

// Delete the key
func (db *DB) del(key []byte) error {
	db.mutex.Lock(key)
	defer db.mutex.Unlock(key)
	err := db.db.Delete(key)
	return err
}

// Store a uint32 as binary data
func (db *DB) putUint32(key []byte, value uint32) error {
	db.mutex.Lock(key)
	defer db.mutex.Unlock(key)
	val := make([]byte, 4)
	binary.LittleEndian.PutUint32(val[:], value)
	err := db.db.Set(key, val)
	return err
}

// Retrieve a binary stored uint32
func (db *DB) getUint32(key []byte) (uint32, error) {
	db.mutex.Lock(key)
	defer db.mutex.Unlock(key)
	data, err := db.db.Get(nil, key)
	if err != nil || data == nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(data), nil
}

// Increment a binary stored uint32
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

// Increment the given string key, the key is created is it doesn't exists
func (db *DB) incrby(key []byte, value int) error {
	db.mutex.Lock(key)
	defer db.mutex.Unlock(key)
	sval, err := db.db.Get(nil, key)
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
	err = db.db.Set(key, []byte(strconv.Itoa(ival+value)))
	return err
}
