/*

Package vkv implements a versioned key value store.

The change history for all keys are kept and versioned by timestamp.

Keys are sorted lexicographically.

## How data is stored ?

Key index (for faster exists check):

    KvKeyIndex + key => 1

Meta index (to track the serialized meta blob for each version of a key, and be able to remove it)

    KvItemMeta + key + version => hash of meta blob

Key value:

    KvItem + (key length as binary encoded uint32) + kv key + index (uint64) => value

Version boundaries for each kv the min/max version is stored for iteration/seeking:

    Meta + KvVersionMin/KvVersionMax + kv key => binary encoded uint64

Number of versions for each key:

    Meta + KvVersionCnt + kv key => binary encoded uint64


*/

package vkv

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/cznic/kv"
)

// Define namespaces for raw key sorted in db.
const (
	Empty byte = iota
	Meta
	KvKeyIndex
	KvMetaIndex
	KvItem
	KvItemMeta
	KvVersionCnt
	KvVersionMin
	KvVersionMax
	KvPrefixMeta // Not in use yet but reserved for future usage
	KvPrefixCnt  // Same here
	KvPrefixStart
	KvPrefixEnd
)

// KvType for meta serialization
const KvType = "kv"

type Flag byte

const (
	Unknown Flag = iota
	Deleted
	HashOnly
	DataOnly
	HashAndData
)

var ErrNotFound = errors.New("vkv: key does not exist")

// // TODO(tsileo): no more `db` embedded into the KeyValue/VkvEntry
// // KeyValue holds a singke key value pair, along with the version (the creation timestamp)
// type KeyValue struct {
// 	Key       string `json:"key,omitempty"`
// 	Value     string `json:"value"`
// 	Version   int    `json:"version"`
// 	db        *DB    `json:"-"`
// 	namespace string `json:"-"`
// }
// func (kvi *KeyValue) SetNamespace(ns string) error {
// 	kvi.namespace = ns
// 	return nil
// }

// func (kvi *KeyValue) Namespace() string {
// 	return kvi.namespace
// }

// KeyValueVersions holds the full history for a key value pair
type KeyValueVersions struct {
	Key string `json:"key"`

	// FIXME(tsileo): turn this into a []*VkvEntry
	Versions []*KeyValue `json:"versions"`
}

type DB struct {
	db   *kv.DB
	path string
	mu   *sync.Mutex
}

// New creates a new database.
func New(path string) (*DB, error) {
	createOpen := kv.Open
	if _, err := os.Stat(path); os.IsNotExist(err) {
		createOpen = kv.Create
	}
	kvdb, err := createOpen(path, &kv.Options{})
	if err != nil {
		return nil, err
	}
	return &DB{
		db:   kvdb,
		path: path,
		mu:   new(sync.Mutex),
	}, nil
}

func (db *DB) Close() error {
	return db.db.Close()
}

func (db *DB) Destroy() error {
	if db.path != "" {
		db.Close()
		return os.RemoveAll(db.path)
	}
	return nil
}

// Store a uint64 as binary data.
func (db *DB) putUint64(key []byte, value uint64) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	val := make([]byte, 8)
	binary.LittleEndian.PutUint64(val[:], value)
	err := db.db.Set(key, val)
	return err
}

// Retrieve a binary stored uint64.
func (db *DB) getUint64(key []byte) (uint64, error) {
	data, err := db.db.Get(nil, key)
	if err != nil || data == nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(data), nil
}

// Increment a binary stored uint64.
func (db *DB) incrUint64(key []byte, step int) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	data, err := db.db.Get(nil, key)
	var value uint64
	if err != nil {
		return err
	}
	if data == nil {
		value = 0
	} else {
		value = binary.LittleEndian.Uint64(data)
	}
	val := make([]byte, 8)
	binary.LittleEndian.PutUint64(val[:], uint64(int(value)+step))
	err = db.db.Set(key, val)
	return err
}

func encodeKey(key []byte, version int) []byte {
	versionbyte := make([]byte, 8)
	binary.BigEndian.PutUint64(versionbyte, uint64(version))
	k := make([]byte, len(key)+13)
	k[0] = KvItem
	binary.LittleEndian.PutUint32(k[1:5], uint32(len(key)))
	copy(k[5:], key)
	copy(k[5+len(key):], versionbyte)
	return k
}

// Extract the index from the raw key
func decodeKey(key []byte) (string, int) {
	klen := int(binary.LittleEndian.Uint32(key[1:5]))
	index := int(binary.BigEndian.Uint64(key[len(key)-8:]))
	member := make([]byte, klen)
	copy(member, key[5:5+klen])
	return string(member), index
}

func encodeMeta(keyByte byte, key []byte) []byte {
	cardkey := make([]byte, len(key)+1)
	cardkey[0] = keyByte
	copy(cardkey[1:], key)
	return cardkey
}

// Get the length of the list
func (db *DB) VersionCnt(key string) (int, error) {
	bkey := []byte(key)
	cardkey := encodeMeta(KvVersionCnt, bkey)
	card, err := db.getUint64(encodeMeta(Meta, cardkey))
	return int(card), err
}

func (db *DB) DeleteVersion(key string, version int) error {
	bkey := []byte(key)
	kmember := encodeKey(bkey, version)
	if err := db.db.Delete(kmember); err != nil {
		return err
	}
	cardkey := encodeMeta(KvVersionCnt, bkey)
	if err := db.incrUint64(encodeMeta(Meta, cardkey), -1); err != nil {
		return err
	}
	card, err := db.getUint64(encodeMeta(Meta, cardkey))
	if err != nil {
		return err
	}
	if card == 0 {
		if err := db.db.Delete(encodeMeta(KvKeyIndex, bkey)); err != nil {
			return err
		}
		if err := db.db.Delete(encodeMeta(KvItemMeta, encodeKey(bkey, version))); err != nil {
			return err
		}
		if err := db.db.Delete(encodeMeta(KvVersionMin, bkey)); err != nil {
			return err
		}
		if err := db.db.Delete(encodeMeta(KvVersionMax, bkey)); err != nil {
			return err
		}
		if err := db.db.Delete(encodeMeta(Meta, cardkey)); err != nil {
			return err
		}
	}
	// TODO delete encodeMeta(KvMetaIndex, []byte(hash)
	return nil
}

// MetaBlobApplied returns true if the meta blob is already applied
func (db *DB) MetaBlobApplied(hash string) (bool, error) {
	res, err := db.db.Get(nil, encodeMeta(KvMetaIndex, []byte(hash)))
	if err != nil {
		return false, err
	}
	if len(res) == 0 {
		return false, nil
	}
	return true, nil
}

// MetaBlob retrns the meta blob where this key/version is stored
func (db *DB) MetaBlob(key string, version int) (string, error) {
	bhash, err := db.db.Get(nil, encodeMeta(KvItemMeta, encodeKey([]byte(key), version)))
	if err != nil {
		return "", err
	}
	return string(bhash), nil
}

func (db *DB) setmetablob(key string, version int, hash string) error {
	if err := db.db.Set(encodeMeta(KvMetaIndex, []byte(hash)), []byte{1}); err != nil {
		return err
	}
	return db.db.Set(encodeMeta(KvItemMeta, encodeKey([]byte(key), version)), []byte(hash))
}

// Put updates the value for the given version associated with key,
// if version == -1, version will be set to time.Now().UTC().UnixNano().
func (db *DB) Put(key, ref string, data []byte, version int) (*KeyValue, error) {
	if version == -1 {
		version = int(time.Now().UTC().UnixNano())
	}
	bkey := []byte(key)
	cmin, err := db.getUint64(encodeMeta(KvVersionMin, bkey))
	if err != nil {
		return nil, err
	}
	cmax, err := db.getUint64(encodeMeta(KvVersionMax, bkey))
	if err != nil {
		return nil, err
	}
	llen := -1
	if cmin == 0 && cmax == 0 {
		llen, err = db.VersionCnt(key)
		if err != nil {
			return nil, err
		}
	}
	if llen == 0 || int(cmin) > version {
		if err := db.putUint64(encodeMeta(KvVersionMin, bkey), uint64(version)); err != nil {
			return nil, err
		}
	}
	if cmax == 0 || int(cmax) < version {
		if err := db.putUint64(encodeMeta(KvVersionMax, bkey), uint64(version)); err != nil {
			return nil, err
		}
	}
	kmember := encodeKey(bkey, version)
	cval, err := db.db.Get(nil, kmember)
	if err != nil {
		return nil, err
	}
	// TODO prevent overwriting an existing version
	if cval == nil {
		cardkey := encodeMeta(KvVersionCnt, bkey)
		if err := db.incrUint64(encodeMeta(Meta, cardkey), 1); err != nil {
			return nil, err
		}
	}
	kv := &KeyValue{
		Hash:    ref,
		Data:    data,
		Version: version,
		Key:     key,
		db:      db,
	}
	value, err := kv.Serialize(false)
	if err != nil {
		return nil, err
	}
	if err := db.db.Set(kmember, []byte(value)); err != nil {
		return nil, err
	}
	if err := db.db.Set(encodeMeta(KvKeyIndex, bkey), []byte{1}); err != nil {
		return nil, err
	}
	return kv, nil
}

func (db *DB) PutPrefix(prefix, key, ref string, data []byte, version int) (*KeyValue, error) {
	// FIXME(tsileo): in meta, check if strings.Contains(s, ":") and PutPrefix instead of Put
	prefixedKey := fmt.Sprintf("%s:%s", prefix, key)
	bkey := []byte(prefixedKey)
	bprefix := []byte(prefix)

	// Track the boundaries for later iteration
	prefixStart, err := db.db.Get(nil, encodeMeta(KvPrefixStart, bprefix))
	if err != nil {
		return nil, err
	}
	if (prefixStart == nil || len(prefixStart) == 0) || bytes.Compare(bkey, prefixStart) < 0 {
		if err := db.db.Set(encodeMeta(KvPrefixStart, bprefix), bkey); err != nil {
			return nil, err
		}
	}
	prefixEnd, err := db.db.Get(nil, encodeMeta(KvPrefixEnd, bprefix))
	if err != nil {
		return nil, err
	}
	// XXX(tsileo): more efficient to save only the key (full key = prefix + key)
	// and rebuilt the keys while iterating a given prefix on the fly.
	if (prefixEnd == nil || len(prefixEnd) == 0) || bytes.Compare(bkey, prefixEnd) > 0 {
		if err := db.db.Set(encodeMeta(KvPrefixEnd, bprefix), bkey); err != nil {
			return nil, err
		}
	}
	// Save the key prefix:key
	return db.Put(prefixedKey, ref, data, version)
}

func (db *DB) Check(key string) (bool, error) {
	bkey := []byte(key)
	exists, err := db.db.Get(nil, encodeMeta(KvKeyIndex, bkey))
	if err != nil {
		return false, err
	}
	if len(exists) == 0 {
		return false, nil
	}
	return true, nil
}

// Get returns the latest value for the given key,
// if version == -1, the latest version will be returned.
func (db *DB) Get(key string, version int) (*KeyValue, error) {
	bkey := []byte(key)
	exists, err := db.db.Get(nil, encodeMeta(KvKeyIndex, bkey))
	if err != nil {
		return nil, err
	}
	if len(exists) == 0 {
		return nil, ErrNotFound
	}
	if version == -1 {
		max, err := db.getUint64(encodeMeta(KvVersionMax, bkey))
		if err != nil {
			return nil, err
		}
		version = int(max)
	}
	val, err := db.db.Get(nil, encodeKey(bkey, version))
	if err != nil {
		return nil, err
	}
	kv, err := Unserialize(key, val)
	if err != nil {
		return nil, err
	}
	kv.Version = version
	return kv, nil
}

// Return a lexicographical range
func (db *DB) Versions(key string, start, end, limit int) (*KeyValueVersions, error) {
	res := &KeyValueVersions{
		Key:      key,
		Versions: []*KeyValue{},
	}
	bkey := []byte(key)
	exists, err := db.db.Get(nil, encodeMeta(KvKeyIndex, bkey))
	if err != nil {
		return nil, err
	}
	if len(exists) == 0 {
		return nil, ErrNotFound
	}
	max, err := db.getUint64(encodeMeta(KvVersionMax, bkey))
	if err != nil {
		return nil, err
	}
	version := int(max)
	min, err := db.getUint64(encodeMeta(KvVersionMin, bkey))
	if err != nil {
		return nil, err
	}
	end = int(min)
	enum, _, err := db.db.Seek(encodeKey(bkey, version))
	if err != nil {
		return nil, err
	}
	endBytes := encodeKey(bkey, end)
	i := 0
	for {
		k, v, err := enum.Prev()
		if err == io.EOF {
			break
		}
		if bytes.Compare(k, endBytes) < 0 || (limit != 0 && i > limit) {
			return res, nil
		}
		_, index := decodeKey(k)
		kv, err := Unserialize(key, v)
		if err != nil {
			return nil, err
		}
		kv.Version = index
		res.Versions = append(res.Versions, kv)
		i++
	}
	return res, nil
}

// Return a lexicographical range
func (db *DB) VersionsOld(key string, start, end, limit int) (*KeyValueVersions, error) {
	// FIXME(tsileo): returns versions in desc by default
	res := &KeyValueVersions{
		Key:      key,
		Versions: []*KeyValue{},
	}
	bkey := []byte(key)
	exists, err := db.db.Get(nil, encodeMeta(KvKeyIndex, bkey))
	if err != nil {
		return nil, err
	}
	if len(exists) == 0 {
		return nil, ErrNotFound
	}
	enum, _, err := db.db.Seek(encodeKey(bkey, start))
	if err != nil {
		return nil, err
	}
	endBytes := encodeKey(bkey, end)
	i := 0
	for {
		k, v, err := enum.Next()
		if err == io.EOF {
			break
		}
		if bytes.Compare(k, endBytes) > 0 || (limit != 0 && i > limit) {
			return res, nil
		}
		_, index := decodeKey(k)
		kv, err := Unserialize(key, v)
		if err != nil {
			return nil, err
		}
		kv.Version = index

		res.Versions = append(res.Versions, kv)
		i++
	}
	return res, nil
}

// Return a lexicographical range
func (db *DB) Keys(start, end string, limit int) ([]*KeyValue, error) {
	res := []*KeyValue{}
	enum, _, err := db.db.Seek(encodeMeta(KvKeyIndex, []byte(start)))
	if err != nil {
		return nil, err
	}
	endBytes := encodeMeta(KvKeyIndex, []byte(end))
	i := 0
	for {
		k, _, err := enum.Next()
		if err == io.EOF {
			break
		}
		if bytes.Compare(k, endBytes) > 0 || (limit != 0 && i > limit) {
			return res, nil
		}
		kv, err := db.Get(string(k[1:]), -1)
		if err != nil {
			return nil, err
		}
		res = append(res, kv)
		i++
	}
	return res, nil
}

// Return a lexicographical range
func (db *DB) ReverseKeys(start, end string, limit int) ([]*KeyValue, error) {
	res := []*KeyValue{}
	enum, _, err := db.db.Seek(encodeMeta(KvKeyIndex, []byte(end)))
	if err != nil {
		return nil, err
	}
	endBytes := encodeMeta(KvKeyIndex, []byte(start))
	i := 0
	for {
		k, _, err := enum.Prev()
		// if i == 0 && bytes.HasPrefix(k, []byte(
		if err == io.EOF {
			break
		}
		if bytes.Compare(k, endBytes) < 0 || (limit != 0 && i > limit) {
			return res, nil
		}
		kv, err := db.Get(string(k[1:]), -1)
		if err != nil {
			return nil, err
		}
		res = append(res, kv)
		i++
	}
	return res, nil
}

// Return a lexicographical range
func (db *DB) ReversePrefixKeys(prefix, start, end string, limit int) ([]*KeyValue, error) {
	res := []*KeyValue{}
	if start == "" {
		// XXX(tsileo): Ensure it works
		prefixStart, err := db.db.Get(nil, encodeMeta(KvPrefixStart, []byte(prefix)))
		// prefixStart, err := db.db.Get(nil, encodeMeta(KvPrefixStart, []byte{}))
		if err != nil {
			return nil, err
		}
		start = string(prefixStart)
	}
	// FIXME(tsileo): a better way to tell we want the end? or \xff is good enough?
	if end == "\xff" {
		prefixEnd, err := db.db.Get(nil, encodeMeta(KvPrefixEnd, []byte(prefix)))
		if err != nil {
			return nil, err
		}
		end = string(prefixEnd)
	}
	enum, _, err := db.db.Seek(encodeMeta(KvKeyIndex, []byte(end)))
	if err != nil {
		return nil, err
	}
	endBytes := encodeMeta(KvKeyIndex, []byte(start))
	i := 0
	for {
		k, _, err := enum.Prev()
		if err == io.EOF {
			break
		}
		if bytes.Compare(k, endBytes) < 0 || (limit != 0 && i > limit) {
			return res, nil
		}
		kv, err := db.Get(string(k[1:]), -1)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch key \"%s\": %v", k[1:], err)
		}
		res = append(res, kv)
		i++
	}
	return res, nil
}

type KeyValue struct {
	Flag Flag `json:"flag"`
	// CustomFlag byte `json:"cflag"`

	Key     string `json:"key,omitempty"`
	Version int    `json:"version"`
	db      *DB    `json:"-"`

	Hash string `json:"hash,omitempty"`
	Data []byte `json:"data,omitempty"`
}

func (kvi *KeyValue) SetMetaBlob(hash string) error {
	return kvi.db.setmetablob(kvi.Key, kvi.Version, hash)
}

func (ve *KeyValue) Type() string {
	return KvType
}

// Implements the `MetaData` interface
func (ve *KeyValue) Dump() ([]byte, error) {
	return ve.Serialize(true)
}

// Implements the `MetaData` interface
func (ve *KeyValue) Serialize(withKey bool) ([]byte, error) {
	// Check if the flag is set
	if ve.Flag == Unknown {
		if ve.Hash != "" && ve.Data != nil {
			ve.Flag = HashAndData
		} else if ve.Hash != "" {
			ve.Flag = HashOnly
		} else if ve.Data != nil {
			ve.Flag = DataOnly
		}
	}

	// Serialize format:
	// Flag + vint len(key) + key + version (4bytes uint32) + custom Flag + Hash (fixed size/optional) + vint len(data) + data

	// Docstore serialize:
	// HashOnly + (Indexed|Deleted|NoFlag) + Hash of the JSON body

	// XXX(tsileo): find a way to do the GC using data via another ExtractFunc interface that returns a list of []Ref?
	// e.g. parse the filetreemeta recursively until we discover all the blobs?

	// Store the 1 byte flag
	var buf bytes.Buffer
	if err := buf.WriteByte(byte(ve.Flag)); err != nil {
		return nil, err
	}

	tmp := make([]byte, 4)
	tmp2 := make([]byte, 8)

	if withKey {
		// The `withKey` option should only be true when dumping the vkv entry in a meta blob

		// Store the key length
		binary.BigEndian.PutUint32(tmp[:], uint32(len(ve.Key)))
		if _, err := buf.Write(tmp); err != nil {
			return nil, err
		}

		// Store the actual key
		if _, err := buf.WriteString(ve.Key); err != nil {
			return nil, err
		}
	}

	// Store the version
	binary.BigEndian.PutUint64(tmp2[:], uint64(ve.Version))
	if _, err := buf.Write(tmp2); err != nil {
		return nil, err
	}

	// Store the custom flag
	// if err := buf.WriteByte(byte(ve.CustomFlag)); err != nil {
	// 	return nil, err
	// }

	// Store the Hash/Data
	switch ve.Flag {
	case HashOnly, HashAndData:
		bhash, err := hex.DecodeString(ve.Hash)
		if err != nil {
			return nil, err
		}
		if _, err := buf.Write(bhash); err != nil {
			return nil, err
		}
		if ve.Flag == HashOnly {
			return buf.Bytes(), nil
		}

		// Execute DataOnly too since the flag in HashAndData
		fallthrough
	case DataOnly:
		binary.BigEndian.PutUint32(tmp[:], uint32(len(ve.Data)))
		if _, err := buf.Write(tmp); err != nil {
			return nil, err
		}
		if _, err := buf.Write(ve.Data); err != nil {
			return nil, err
		}

	}
	return buf.Bytes(), nil
}

func UnserializeBlob(data []byte) (*KeyValue, error) {
	fmt.Printf("unser %d / %v\n", len(data), string(data))
	r := bytes.NewReader(data)
	tmp := make([]byte, 4)
	tmp2 := make([]byte, 8)

	// Read the first flag
	flag, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	// Read the Key size
	if _, err := r.Read(tmp); err != nil {
		return nil, err
	}
	keySize := int(binary.BigEndian.Uint32(tmp[:]))
	fmt.Printf("ksize=%d\n", keySize)

	// Read the key (now that we know the size)
	bkey := make([]byte, keySize)
	if _, err := r.Read(bkey); err != nil {
		return nil, err
	}
	fmt.Printf("key read = %v\n", string(bkey))

	// Read the version
	if _, err := r.Read(tmp2); err != nil {
		return nil, err
	}
	version := int(binary.BigEndian.Uint64(tmp2[:]))

	// Custom flag
	// cflag, err := r.ReadByte()
	// if err != nil {
	// 	return nil, err
	// }

	ve := &KeyValue{
		Key:     string(bkey),
		Version: version,
		Flag:    Flag(flag),
		// CustomFlag: cflag,
	}
	fmt.Printf("ve=%+v", ve)
	// Read the Hash/Data according to the set Flag
	switch ve.Flag {
	case HashOnly, HashAndData:
		bhash := make([]byte, 32)
		if _, err := r.Read(bhash); err != nil {
			return nil, err
		}
		ve.Hash = hex.EncodeToString(bhash)
		if ve.Flag == HashOnly {
			return ve, nil
		}
		fallthrough
	case DataOnly:
		if _, err := r.Read(tmp); err != nil {
			return nil, err
		}
		size := int(binary.BigEndian.Uint32(tmp[:]))
		fmt.Printf("data size=%d\n", size)
		data := make([]byte, size)
		if _, err := r.Read(data); err != nil {
			return nil, err
		}
		ve.Data = data
	}
	return ve, nil
}

func Unserialize(key string, data []byte) (*KeyValue, error) {
	r := bytes.NewReader(data)
	tmp := make([]byte, 4)
	tmp2 := make([]byte, 8)

	// Read the first flag
	flag, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	// Read the version
	if _, err := r.Read(tmp2); err != nil {
		return nil, err
	}
	version := int(binary.BigEndian.Uint64(tmp2[:]))

	// Custom flag
	// cflag, err := r.ReadByte()
	// if err != nil {
	// 	return nil, err
	// }
	ve := &KeyValue{
		Key:     key,
		Version: version,
		Flag:    Flag(flag),
		// CustomFlag: cflag,
	}
	// Read the Hash/Data according to the set Flag
	switch ve.Flag {
	case HashOnly, HashAndData:
		bhash := make([]byte, 32)
		if _, err := r.Read(bhash); err != nil {
			return nil, err
		}
		ve.Hash = hex.EncodeToString(bhash)
		if ve.Flag == HashOnly {
			return ve, nil
		}
		fallthrough
	case DataOnly:
		if _, err := r.Read(tmp); err != nil {
			return nil, err
		}
		size := int(binary.BigEndian.Uint32(tmp[:]))
		data := make([]byte, size)
		if _, err := r.Read(data); err != nil {
			return nil, err
		}
		ve.Data = data
	}
	return ve, nil
}
