package util

import (
	"bytes"
	"io"
	"os"
	"path/filepath"

	"github.com/cznic/kv"
)

func opts() *kv.Options {
	return &kv.Options{
		VerifyDbBeforeOpen:  true,
		VerifyDbAfterOpen:   true,
		VerifyDbBeforeClose: true,
		VerifyDbAfterClose:  true,
	}
}

var (
	XDGDataHome = filepath.Join(os.Getenv("HOME"), ".local/share")
	DBPath      = filepath.Join(XDGDataHome, "datadb-glacier-db")
)

type DB struct {
	kvDB *kv.DB
}

func (db *DB) Set(key, value string) error {
	return db.kvDB.Set([]byte(key), []byte(value))
}

func (db *DB) Delete(key string) error {
	return db.kvDB.Delete([]byte(key))
}

func (db *DB) Get(key string) (value string, err error) {
	val, err := db.kvDB.Get(nil, []byte(key))
	return string(val), err
}

func (db *DB) Close() {
	db.kvDB.Close()
}

func (db *DB) Drop() {
	kvs := make(chan *KeyValue)
	go db.Iter(kvs, "", "\xff", 0)
	for kv := range kvs {
		db.Delete(kv.Key)
	}
}

type KeyValue struct {
	Key   string
	Value string
}

func (db *DB) Iter(res chan<- *KeyValue, start, end string, limit int) error {
	defer close(res)
	kStart := []byte(start)
	kEnd := []byte(end)
	enum, _, err := db.kvDB.Seek(kStart)
	if err != nil {
		return err
	}
	endBytes := kEnd
	i := 0
	for {
		k, v, err := enum.Next()
		if err == io.EOF {
			break
		}
		if bytes.Compare(k, endBytes) > 0 || (limit != 0 && i > limit) {
			return nil
		}
		res <- &KeyValue{string(k), string(v)}
		i++
	}
	return nil
}

func GetDB() (*DB, error) {
	if err := os.MkdirAll(XDGDataHome, 0700); err != nil {
		return nil, err
	}
	createOpen := kv.Open
	if _, err := os.Stat(DBPath); os.IsNotExist(err) {
		createOpen = kv.Create
	}
	db, err := createOpen(DBPath, opts())
	if err != nil {
		return nil, err
	}
	return &DB{db}, nil
}
