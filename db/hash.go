package db

import (
	"encoding/binary"
	"errors"
	"bytes"
)

//
// ## Hash
// hash field
//   Hash + (key length as binary encoded uint32) + hash key + field  => value
// fields count
//   Meta + HashFieldsCount + hash key => binary encoded uint32
// the total number of hashes
//   Meta + HashCnt => binary encoded uint32
// an index to perform a range of hash
//   Meta + HashIndex + key => empty
//

// Format a hash field
func keyHashField(key []byte, field interface{}) []byte {
	var fieldbyte []byte
	switch k := field.(type) {
	case []byte:
		fieldbyte = k
	case string:
		fieldbyte = []byte(k)
	case byte:
		fieldbyte = []byte{k}
	}
	var buf bytes.Buffer
	buf.Write([]byte{Hash})
	l := make([]byte, 4)
	binary.LittleEndian.PutUint32(l, uint32(len(key)))
	buf.Write(l)
	buf.Write(key)
	buf.Write(fieldbyte)
	return buf.Bytes()
}

// Extract the field from a raw key
func decodeKeyHashField(key []byte) []byte {
	// The first byte is already remove
	klen := int(binary.LittleEndian.Uint32(key[0:4]))
	cpos := 4 + klen
	member := make([]byte, len(key) - cpos)
	copy(member[:], key[cpos:])
	return member
}

// Create the key used to index all hashes key
func keyHashIndex(key []byte) []byte {
	ikey := make([]byte, len(key) + 2)
	ikey[0] = Meta
	ikey[1] = HashIndex
	copy(ikey[2:], key)
	return ikey
}

// Extract the hash key from the raw key
func decodeKeyHashIndex(key []byte) []byte {
	// the Meta byte is already removed from range
	index := make([]byte, len(key) - 1)
	copy(index[:], key[1:])
	return index
}

// Create the key to retrieve the number of field of the hash
func hashFieldsCnt(key []byte) []byte {
	cardkey := make([]byte, len(key) + 1)
	cardkey[0] = HashFieldsCnt
	copy(cardkey[1:], key)
	return cardkey
}

// Returns the number of fields
func (db *DB) Hlen(key string) (int, error) {
	bkey := []byte(key)
	cardkey := hashFieldsCnt(bkey)
	card, err := db.getUint32(KeyType(cardkey, Meta))
	return int(card), err
}

// Set field to value
func (db *DB) Hset(key, field, value string) (int, error) {
	bkey := []byte(key)
	cnt := 0
	kfield := keyHashField(bkey, field)
	cval, _ := db.get(kfield)
	if cval == nil {
		cnt++
	}
	db.put(kfield, []byte(value))
	cardkey := hashFieldsCnt(bkey)
	db.incrUint32(KeyType(cardkey, Meta), cnt)
	db.put(keyHashIndex(bkey), []byte{})
	return cnt, nil
}

func (db *DB) Hmset(key string, fieldvalue ...string) (int, error) {
	bkey := []byte(key)
	cnt := 0
	if len(fieldvalue) % 2 != 0 {
		return cnt, errors.New("Hmset invalid args cnt")
	}
	for i := 0; i < len(fieldvalue); i = i + 2 {
		field := fieldvalue[i] 
		value := fieldvalue[i+1]
		kfield := keyHashField(bkey, field)
		cval, _ := db.get(kfield)
		if cval == nil {
			cnt++
		}
		db.put(kfield, []byte(value))
	}
	cardkey := hashFieldsCnt(bkey)
	db.incrUint32(KeyType(cardkey, Meta), cnt)
	db.put(keyHashIndex(bkey), []byte{})
	return cnt, nil
}



// Test for field existence
func (db *DB) Hexists(key, field string) (int, error) {
	bkey := []byte(key)
	cval, err := db.get(keyHashField(bkey, field))
	cnt := 0
	if cval != nil {
		cnt++
	}
	return cnt, err
}

// Return the given field
func (db *DB) Hget(key, field string) ([]byte, error) {
	bkey := []byte(key)
	cval, err := db.get(keyHashField(bkey, field))
	return cval, err
}

func (db *DB) Hgetall(key string) ([]*KeyValue, error) {
	bkey := []byte(key)
	hkvs := []*KeyValue{}
	start := keyHashField(bkey, []byte{})
	end := keyHashField(bkey, "\xff")
	kvs, err := GetRange(db.db, start, end, 0)
	for _, kv := range kvs {
		ckv := &KeyValue{string(decodeKeyHashField([]byte(kv.Key))), kv.Value}
		hkvs = append(hkvs, ckv)
	}
	return hkvs, err
}

func (db *DB) Hscan(start, end string, limit int) ([][]byte, error) {
	hkeys := [][]byte{}
	kStart := keyHashIndex([]byte(start))
	kEnd := keyHashIndex([]byte(end))
	kvs, err := GetRange(db.db, kStart, kEnd, limit)
	for _, kv := range kvs {
		hkeys = append(hkeys, decodeKeyHashIndex([]byte(kv.Key)))
	}
	return hkeys, err
}

// Hhash
// Hdel
// Hmset
// Hmget
