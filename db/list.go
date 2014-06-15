package db

import (
	"encoding/binary"
)

//
// ## List
// List of strings sorted by an uint32 index key,
// (quite similar to set (but only indexes are unique), the set member is the index,
// and instead of an empty value, the value is stored)
// lists
//   List + (key length as binary encoded uint32) + list key + index (uint32)  => value
// list len
//   Meta + ListLen + list key => binary encoded uint32
// the total number of list
//   Meta + ListCnt => binary encoded uint32
//

// Format the key to add new element to the list at the given index
func keyList(key []byte, index interface{}) []byte {
	indexbyte := make([]byte, 4)
	switch k := index.(type) {
	case []byte:
		indexbyte = k
	case string:
		copy(indexbyte, []byte(k))
	case byte:
		copy(indexbyte, []byte{k})
	case int:
		binary.BigEndian.PutUint32(indexbyte, uint32(k))
	}
	k := make([]byte, len(key)+9)
	k[0] = List
	binary.LittleEndian.PutUint32(k[1:5], uint32(len(key)))
	copy(k[5:], key)
	copy(k[5+len(key):], indexbyte)
	return k
}

// Extract the index from the raw key
func decodeListIndex(key []byte) int {
	return int(binary.BigEndian.Uint32(key[len(key)-4:]))
}

func decodeListKey(key []byte) []byte {
	// The first byte is already remove
	klen := int(binary.LittleEndian.Uint32(key[0:4]))
	member := make([]byte, klen)
	copy(member, key[4:4+klen])
	return member
}

// Build the key to retrieve the list length
func listLen(key []byte) []byte {
	cardkey := make([]byte, len(key)+1)
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
	cval, err := db.get(kmember)
	if err != nil {
		return err
	}
	if err := db.put(kmember, []byte(value)); err != nil {
		return err
	}
	if cval == nil {
		cardkey := listLen(bkey)
		if err := db.incrUint32(KeyType(cardkey, Meta), 1); err != nil {
			return err
		}
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
		if kv.Value != "\x00" {
			res = append(res, []byte(kv.Value))
		}
		//res = append(res,  decodeListIndex([]byte(kv.Key)))
	}
	return res, nil
}

// Returns list values, sorted by index ASC
func (db *DB) LiterWithIndex(key string) (ivs []*IndexValue, err error) {
	bkey := []byte(key)
	start := keyList(bkey, []byte{})
	end := keyList(bkey, "\xff")
	kvs, err := GetRange(db.db, start, end, 0)
	if err != nil {
		return
	}
	for _, skv := range kvs {
		if skv.Value != "\x00" {
			ivs = append(ivs, &IndexValue{Index: decodeListIndex([]byte(skv.Key)), Value: skv.Value})
		}
	}
	return
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

// Return a lexicographical range
func (db *DB) GetListRange(key, kStart string, kEnd string, limit int) (kvs []*KeyValue, err error) {
	// TODO(tsileo) make kStart, kEnd int instead of string
	bkey := []byte(key)
	kvs, _ = GetRange(db.db, keyList(bkey, kStart), keyList(bkey, kEnd), limit)
	return
}

// Return a lexicographical range
func (db *DB) GetListRangeLast(key, kStart string, kEnd string, limit int) (kv *KeyValue, err error) {
	// TODO(tsileo) make kStart, kEnd int instead of string
	bkey := []byte(key)
	kv, _ = GetRangeLast(db.db, keyList(bkey, kStart), keyList(bkey, kEnd), limit)
	return
}

func (db *DB) Lmrange2(key string, kStart, kEnd int) (ivs []*IndexValue, err error) {
	fullIvs, err := db.LiterWithIndex(key)
	if err != nil {
		return ivs, err
	}
	for _, iv := range fullIvs {
		if iv.Index > kStart {
			ivs = append(ivs, iv)
		}
		if iv.Index > kEnd {
			break
		}
	}
	return ivs, nil
}

func (db *DB) Lmrange(key string, kStart, kEnd int) (ivs []*IndexValue, err error) {
	bkey := []byte(key)
	start := keyList(bkey, kStart)
	end := keyList(bkey, "\xff")
	kvs, err := GetRange(db.db, start, end, 0)
	if err != nil {
		return
	}
	for _, skv := range kvs {
		if skv.Value != "\x00" {
			civ := &IndexValue{Index: decodeListIndex([]byte(skv.Key)), Value: skv.Value}
			ivs = append(ivs, civ)
			if civ.Index > kEnd {
				break
			}
		}
	}
	return
}

// func (db *DB) Srange(snapId, kStart string, kEnd string, limit int) [][]byte
// func (db *DB) Srem(key string, member ...string) int
