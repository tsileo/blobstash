package db

import (
	"encoding/binary"
	"strconv"
)

//
// ## List
// List of strings, but sorted by an uint32 index key instead of insertion order,
// quite like a zset, but member have no unique constraint.
// (quite similar to set (but only indexes are unique), the set member is the index,
// and instead of an empty value, the value is stored)
// lists
//   List + (key length as binary encoded uint32) + list key + index (uint)  => value
// list len
//   Meta + ListLen + list key => binary encoded uint32
// the total number of list
//   Meta + ListCnt => binary encoded uint32
//

// Format the key to add new element to the list at the given index
func keyList(key []byte, index interface{}) []byte {
	var indexbyte []byte
	switch k := index.(type) {
	case []byte:
		indexbyte = k
	case string:
		indexbyte = []byte(k)
	case byte:
		indexbyte = []byte{k}
	case int:
		indexbyte = make([]byte, 4)
		binary.BigEndian.PutUint32(indexbyte[:], uint32(k))
	}
	k := make([]byte, len(key) + 9)
	k[0] = List
	binary.LittleEndian.PutUint32(k[1:5], uint32(len(key)))
	cpos := 5 + len(key)
	copy(k[5:cpos], key)
	copy(k[cpos:cpos+4], indexbyte)
	return k
}

// Extract the index from the raw key
func decodeListIndex(key []byte) int {
	// The first byte is already remove
	cpos := int(binary.LittleEndian.Uint32(key[0:4])) + 4
	member := make([]byte, len(key) -  cpos)
	copy(member[:], key[cpos:])
	index, _ := strconv.Atoi(string(member))
	return index
}

// Build the key to retrieve the list length
func listLen(key []byte) []byte {
	cardkey := make([]byte, len(key) + 1)
	cardkey[0] = ListLen
	copy(cardkey[1:], key)
	return cardkey
}

// Get the length of the list
func (db *DB) Llen(key string) (int, error) {
	bkey := []byte(key)
	cardkey := listLen(bkey)
	card, err := db.getUint32(KeyType(cardkey, Meta))
	return int(card), err
}

// Add an element in the list at the given index
func (db *DB) Ladd(key string, index int, value string) error {
	bkey := []byte(key)
	kmember := keyList(bkey, index)
	cval, _ := db.get(kmember)
	db.put(kmember, []byte(value))
	if cval == nil {
		cardkey := listLen(bkey)
		db.incrUint32(KeyType(cardkey, Meta), 1)
	}
	return nil
}

// Returns the value at the given index
func (db *DB) Lindex(key string, index int) ([]byte, error) {
	bkey := []byte(key)
	cval, err := db.get(keyList(bkey, index))
	return cval, err
}

// Returns list values, sorted by index ASC
func (db *DB) Liter(key string) ([][]byte, error) {
	bkey := []byte(key)
	start := keyList(bkey, []byte{})
	end := keyList(bkey, "\xff")
	res := [][]byte{}
	kvs, err := GetRange(db.db, start, end, 0) 
	if err != nil {
		return res, err
	}	
	for _, kv := range kvs {
		res = append(res, []byte(kv.Value))
		//res = append(res,  decodeListIndex([]byte(kv.Key)))
	}
	return res, nil
}

// Delete the entire list
func (db *DB) Ldel(key string) error {
	bkey := []byte(key)
	start := keyList(bkey, []byte{})
	end := keyList(bkey, "\xff")
	kvs, err := GetRange(db.db, start, end, 0) 
	if err != nil {
		return err
	}
	for _, kv := range kvs {
		err := db.del([]byte(kv.Key))
		if err != nil {
			return err
		}
	}
	cardkey := listLen(bkey)
	err = db.del(KeyType(cardkey, Meta))
	return err
}

// Return a lexicographical range from a snapshot
func (db *DB) GetListRange(key, kStart string, kEnd string, limit int) (kvs []*KeyValue, err error) {
	bkey := []byte(key)
	kvs, _ = GetRange(db.db, keyList(bkey, kStart), keyList(bkey, kEnd), limit)
	return
}

// func (db *DB) Srange(snapId, kStart string, kEnd string, limit int) [][]byte
// func (db *DB) Srem(key string, member ...string) int
