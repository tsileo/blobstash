package db

import (
	"encoding/binary"
)

//
// ## Hash
// hash field
//   Hash + (key length as binary encoded uint32) + hash key + field  => value
// fields count
//   Meta + HashFieldsCount + hash key => binary encoded uint32
// the total number of hashes
//   Meta + HashCnt => binary encoded uint32
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
	k := make([]byte, len(fieldbyte) + len(key) + 5)
	k[0] = Hash
	binary.LittleEndian.PutUint32(k[1:5], uint32(len(key)))
	cpos := 5 + len(key)
	copy(k[5:cpos], key)
	if len(fieldbyte) > 0 {
		copy(k[cpos:], fieldbyte)
	}
	return k
}

func decodeKeyHashField(key []byte) []byte {
	// The first byte is already remove
	cpos := int(binary.LittleEndian.Uint32(key[1:5])) + 5
	member := make([]byte, len(key) -  cpos)
	copy(member[:], key[cpos:])
	return member
}

func hashFieldsCnt(key []byte) []byte {
	cardkey := make([]byte, len(key) + 1)
	cardkey[0] = HashFieldsCnt
	copy(cardkey[1:], key)
	return cardkey
}

//   Set + (key length as binary encoded uint32) + set key + set member  => empty
func (db *DB) Hlen(key string) int {
	bkey := []byte(key)
	cardkey := hashFieldsCnt(bkey)
	card := db.getUint32(KeyType(cardkey, Meta))
	return int(card)
}


func (db *DB) Hset(key, field, value string) (int, error) {
	bkey := []byte(key)
	db.mutex.Lock(bkey)
	defer db.mutex.Unlock(bkey)
	cnt := 0
	kfield := keyHashField(bkey, field)
	cval, _ := db.ldb.Get(db.ro, kfield)
	if cval == nil {
		cnt++
	}
	db.ldb.Put(db.wo, kfield, []byte(value))
	cardkey := hashFieldsCnt(bkey)
	db.incrUint32(cardkey, cnt)
	return cnt, nil
}

func (db *DB) Hexists(key, field string) int {
	bkey := []byte(key)
	db.mutex.Lock(bkey)
	defer db.mutex.Unlock(bkey)
	cval, _ := db.ldb.Get(db.ro, keyHashField(bkey, field))
	cnt := 0
	if cval != nil {
		cnt++
	}
	return cnt
}

func (db *DB) Hget(key, field string) ([]byte, error) {
	bkey := []byte(key)
	db.mutex.Lock(bkey)
	defer db.mutex.Unlock(bkey)
	cval, err := db.ldb.Get(db.ro, keyHashField(bkey, field))
	return cval, err
}

// Hid
// Hdel
// Hmset
// Hmget
