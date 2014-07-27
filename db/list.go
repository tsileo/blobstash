package db

import (
	"encoding/binary"
	"fmt"
)

//
// ## List
// List of strings sorted by an uint32 index key,
// (quite similar to set (but only indexes are unique), the set member is the index,
// and instead of an empty value, the value is stored)
// lists
//   List + (key length as binary encoded uint32) + list key + index (uint32)  => value
// List boundaries: for each list the min/max index are stored for iteration/seeking
//  Meta + ListMax/ListMin + list key => binary encoded uint32
// list len
//   Meta + ListLen + list key => binary encoded uint32
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
	klen := int(binary.LittleEndian.Uint32(key[0:4]))
	if len(key) == klen+4 {
		return 0
	}
	return int(binary.BigEndian.Uint32(key[len(key)-4:]))
}

func decodeListKey(key []byte) []byte {
	// The first byte is already remove
	klen := int(binary.LittleEndian.Uint32(key[0:4]))
	member := make([]byte, klen)
	copy(member, key[4:4+klen])
	return member
}

// Build a list meta key
func listMeta(keyByte byte, key []byte) []byte {
	cardkey := make([]byte, len(key)+1)
	cardkey[0] = keyByte
	copy(cardkey[1:], key)
	return cardkey
}

// Get the length of the list
func (db *DB) Llen(key string) (int, error) {
	bkey := []byte(key)
	cardkey := listMeta(ListLen, bkey)
	card, err := db.getUint32(KeyType(cardkey, Meta))
	return int(card), err
}



func (db *DB) Ladd(key string, index int, value string) error {
	bkey := []byte(key)
	cmin, err := db.getUint32(listMeta(ListMin, bkey))
	if err != nil {
		return err
	}
	cmax, err := db.getUint32(listMeta(ListMax, bkey))
	if err != nil {
		return err
	}
	llen := -1
	if cmin == 0 && cmax == 0 {
		llen, err = db.Llen(key)
		if err != nil {
			return err
		}
	}
	if llen == 0 || int(cmin) > index {
		if err := db.putUint32(listMeta(ListMin, bkey), uint32(index)); err != nil {
			return fmt.Errorf("Error putUint32 %v", err)
		}
	}
	if cmax == 0 || int(cmax) < index {
		if err := db.putUint32(listMeta(ListMax, bkey), uint32(index)); err != nil {
			return err
		}
	}
	kmember := keyList(bkey, index)
	cval, err := db.get(kmember)
	if err != nil {
		return err
	}
	if err := db.put(kmember, []byte(value)); err != nil {
		return err
	}
	if cval == nil {
		cardkey := listMeta(ListLen, bkey)
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
	min, max, err := db.lMinMax(bkey)
	if err != nil {
		return nil, err
	}
	start := keyList(bkey, min)
	end := keyList(bkey, max)
	res := [][]byte{}
	kvs, err := GetRange(db.db, start, end, 0)
	if err != nil {
		return res, err
	}
	for _, kv := range kvs {
		if kv.Value != "\x00" && kv.Value != "" {
			res = append(res, []byte(kv.Value))
		}
		//res = append(res,  decodeListIndex([]byte(kv.Key)))
	}
	return res, nil
}

func (db *DB) lMinMax(bkey []byte) (int, int, error) {
	min, err := db.getUint32(listMeta(ListMin, bkey))
	if err != nil {
		return 0, 0, err
	}
	max, err := db.getUint32(listMeta(ListMax, bkey))
	if err != nil {
		return 0, 0, err
	}
	return int(min), int(max), nil
}

// Returns list values, sorted by index ASC
func (db *DB) Lriter(key string) ([][]byte, error) {
	bkey := []byte(key)
	min, max, err := db.lMinMax(bkey)
	start := keyList(bkey, min)
	end := keyList(bkey, max)
	res := [][]byte{}
	kvs, err := GetReverseRange(db.db, end, start, 0)
	if err != nil {
		return res, err
	}
	for _, kv := range kvs {
		res = append(res, []byte(kv.Value))
	}
	return res, nil
}

func (db *DB) Llast(key string) ([]byte, error) {
	bkey := []byte(key)
	max, err := db.getUint32(listMeta(ListMax, bkey))
	if err != nil {
		return nil, err
	}
	return db.get(keyList(bkey, int(max)))
}

func (db *DB) LlastWithIndex(key string) (*IndexValue, error) {
	bkey := []byte(key)
	max, err := db.getUint32(listMeta(ListMax, bkey))
	if err != nil {
		return nil, err
	}
	val, err := db.get(keyList(bkey, int(max)))
	if err != nil {
		return nil, err
	}
	return &IndexValue{
		Index: int(max),
		Value: string(val),
	}, nil
}

// Returns list values, sorted by index ASC
func (db *DB) LiterWithIndex(key string) (ivs []*IndexValue, err error) {
	bkey := []byte(key)
	min, max, err := db.lMinMax(bkey)
	if err != nil {
		return nil, err
	}
	start := keyList(bkey, min)
	end := keyList(bkey, max)
	kvs, err := GetRange(db.db, start, end, 0)
	if err != nil {
		return
	}
	for _, skv := range kvs {
		ivs = append(ivs, &IndexValue{Index: decodeListIndex([]byte(skv.Key)), Value: skv.Value})
	}
	return
}

// Returns list values, sorted by index ASC
func (db *DB) LriterWithIndex(key string) (ivs []*IndexValue, err error) {
	bkey := []byte(key)
	min, max, err := db.lMinMax(bkey)
	if err != nil {
		return nil, err
	}
	start := keyList(bkey, min)
	end := keyList(bkey, max)
	kvs, err := GetReverseRange(db.db, end, start, 0)
	if err != nil {
		return
	}
	for _, skv := range kvs {
		ivs = append(ivs, &IndexValue{Index: decodeListIndex([]byte(skv.Key)), Value: skv.Value})
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
	cardkey := listMeta(ListLen, bkey)
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
