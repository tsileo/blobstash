package db

import ()

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

func (db *DB) Getset(key, value string) (val []byte, err error) {
	val, err = db.getset(KeyType(key, String), []byte(value))
	return
}

// Sets the value for a given key.
func (db *DB) Put(key, value string) error {
	_, err := db.getset(KeyType(key, String), []byte(value))
	return err
}

// Delete the given string key
func (db *DB) Del(key string) {
	k := KeyType(key, String)
	db.del(k)
	return
}

// Return a lexicographical range from a snapshot
func (db *DB) GetStringRange(kStart, kEnd string, limit int) (kvs []*KeyValue, err error) {
	kvs, _ = GetRange(db.db, KeyType(kStart, String), KeyType(kEnd, String), limit)
	return
}
