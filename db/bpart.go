package db

import (
	"encoding/binary"
	"github.com/jmhodges/levigo"
	"strconv"
	"log"
)

//
// ## Backup Parts
// (quite similar to set, but the set member is the index,
// and instead of an empty value, the hash of the part is stored)
// part
//   Set + (key length as binary encoded uint32) + part key + part index (uint byte enocded)  => part hash
// parts cardinality
//   Meta + BackupPartCardinality + part key => binary encoded uint32
// the total number of part
//   Meta + BackupPartCnt => binary encoded uint32
//

// Format a set member
func keyBackupPart(key []byte, index interface{}) []byte {
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
	k[0] = BackupPart
	binary.LittleEndian.PutUint32(k[1:5], uint32(len(key)))
	cpos := 5 + len(key)
	copy(k[5:cpos], key)
	copy(k[cpos:cpos+4], indexbyte)
	return k
}

func decodeBackupPartInt(key []byte) int {
	// The first byte is already remove
	cpos := int(binary.LittleEndian.Uint32(key[0:4])) + 4
	member := make([]byte, len(key) -  cpos)
	copy(member[:], key[cpos:])
	index, _ := strconv.Atoi(string(member))
	return index
}

func backupPartCard(key []byte) []byte {
	cardkey := make([]byte, len(key) + 1)
	cardkey[0] = BackupPartCardinality
	copy(cardkey[1:], key)
	return cardkey
}

func (db *DB) Bpcard(key string) (int, error) {
	bkey := []byte(key)
	cardkey := backupPartCard(bkey)
	card := db.getUint32(KeyType(cardkey, Meta))
	return int(card), nil
}


func (db *DB) Bpadd(key string, index int, hash string) error {
	bkey := []byte(key)
	db.mutex.Lock(bkey)
	defer db.mutex.Unlock(bkey)
	kmember := keyBackupPart(bkey, index)
	cval, _ := db.ldb.Get(db.ro, kmember)
	db.ldb.Put(db.wo, kmember, []byte(hash))
	if cval == nil {
		cardkey := backupPartCard(bkey)
		db.incrUint32(KeyType(cardkey, Meta), 1)
	}
	return nil
}

func (db *DB) Bpget(key string, index int) ([]byte, error) {
	bkey := []byte(key)
	db.mutex.Lock(bkey)
	defer db.mutex.Unlock(bkey)
	cval, err := db.ldb.Get(db.ro, keyBackupPart(bkey, index))
	return cval, err
}

func (db *DB) Bparts(key string) [][]byte {
	bkey := []byte(key)
	db.mutex.Lock(bkey)
	snap := db.ldb.NewSnapshot()
	db.mutex.Unlock(bkey)
	defer db.ldb.ReleaseSnapshot(snap)
	ro := levigo.NewReadOptions()
	ro.SetSnapshot(snap)
	defer ro.Close()
	start := keyBackupPart(bkey, []byte{})
	end := keyBackupPart(bkey, "\xff")
	kvs, _ := GetRange(db.ldb, ro, start, end, 0) 
	res := [][]byte{}
	for _, kv := range kvs {
		res = append(res, []byte(kv.Value))
		//res = append(res,  decodeBackupPartInt([]byte(kv.Key)))
	}
	return res
}


// Return a lexicographical range from a snapshot
func (db *DB) GetBpartRange(snapId, key, kStart string, kEnd string, limit int) (kvs []*KeyValue, err error) {
	bkey := []byte(key)
	snap, snapExists := db.GetSnapshot(snapId)
	if snapExists {
		ro := levigo.NewReadOptions()
		ro.SetSnapshot(snap)
		defer ro.Close()
		kvs, _ = GetRange(db.ldb, ro, keyBackupPart(bkey, kStart), keyBackupPart(bkey, kEnd), limit)
	}
	db.UpdateSnapshotTTL(snapId, SnapshotTTL)
	return
}


// func (db *DB) Srange(snapId, kStart string, kEnd string, limit int) [][]byte
// func (db *DB) Srem(key string, member ...string) int
