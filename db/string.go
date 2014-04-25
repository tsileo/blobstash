package db

import (
	"github.com/jmhodges/levigo"
)

// Return the number of string key stored
func (db *DB) GetStringCnt() (uint32, error) {
	return db.getUint32(KeyType(StringCnt, Meta))
}

// Increment the key by the given value,
// just return the value, the result must be set by a raft command
func (db *DB) IncrBy(key string, value int) {
	db.incrby([]byte(key), value)
	return
}

// Retrieves the value for a given key.
func (db *DB) Get(key string) (val []byte, err error) {
	val, err = db.get(KeyType(key, String))
	return
}

func (db *DB) Getset(key string, value string) (val []byte, err error) {
	val, err = db.getset(KeyType(key, String), []byte(value))
	return
}

// Sets the value for a given key.
func (db *DB) Put(key string, value string) error {
	// Incr the StringCnt if needed
	cval, err := db.getset(KeyType(key, String), []byte(value))
	if cval == nil {
		err = db.incrUint32(KeyType(StringCnt, Meta), 1)
	}
	return err
}

// Delete the given string key
func (db *DB) Del(key string) {
	k := KeyType(key, String)
	db.del(k)
	db.incrUint32(KeyType(StringCnt, Meta), -1)
	return
}

// Return a lexicographical range from a snapshot
func (db *DB) GetStringRange(snapId, kStart string, kEnd string, limit int) (kvs []*KeyValue, err error) {
	snap, snapExists := db.GetSnapshot(snapId)
	if snapExists {
		ro := levigo.NewReadOptions()
		ro.SetSnapshot(snap)
		defer ro.Close()
		kvs, _ = GetRange(db.ldb, ro, KeyType(kStart, String), KeyType(kEnd, String), limit)
	}
	db.UpdateSnapshotTTL(snapId, SnapshotTTL)
	return
}
