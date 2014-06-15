/*

Package disklru implements a disk-based LRU cache.

A small key-value store (powered by kv [1]) keeps track of the keys/size/last access time of each time,
and data are stored in files (one file per blob, in nested directory like
"blobs/07/077f2cfed67c84d8785515481766669a3a734bd6".

Eviction is triggered when the cache size exceed a limit,
it tries to keep the size of the cache under a fixed threshold and remove lest recently used item first.

Index

Data are stored the following way (all the uint32 are binary encoded):

	- Index byte + last access time (uint32) + key => size (uint32)
	- Keys byte + key => current access time
	- MetaCnt byte => number of items currently in the cache (uint32)
	- MetaSize byte => total size of the cache (uint32)

Links

	[1] https://github.com/cznic/kv


*/
package disklru

import (
	"bytes"
	"encoding/binary"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/cznic/kv"
)

func dirsPermutations() []string {
    arr := make([]string,0)
    force("01234567890abcdef", 2, "", func(thisString string){
    	arr = append(arr, thisString)
    })
    return arr
}

// Generate string permutation
// from https://gist.github.com/sanikeev/6952900
func force(str string, n int, thisString string,
           callback func(thisString string)) {
    if n == 0 {
        callback(thisString);
        return;
    }
    for _, v := range(str) {
        force(str, n-1, thisString + string(v), callback)
    }
}
// Namespaces for the DB keys
const (
	Keys byte = iota
	Index
	MetaSize
	MetaCnt
)

func opts() *kv.Options {
	return &kv.Options{
		VerifyDbBeforeOpen:  true,
		VerifyDbAfterOpen:   true,
		VerifyDbBeforeClose: true,
		VerifyDbAfterClose:  true,
	}
}

type DiskLRU struct {
	Func      func(string) []byte
	Threshold int64
	db        *kv.DB
	path      string
	sync.Mutex
}

type CacheItem struct {
	Key   string
	Index uint32
	Size  uint32
}

// New initialize a new DiskLRU.
func New(path string, f func(string) []byte, threshold int64) (*DiskLRU, error) {
	db_path := filepath.Join(path, "db")
	if err := os.MkdirAll(path, 0700); err != nil {
		return nil, err
	}
	createOpen := kv.Open
	if _, err := os.Stat(db_path); os.IsNotExist(err) {
		createOpen = kv.Create
		for _, bucket := range dirsPermutations() {
			os.MkdirAll(filepath.Join(path, "blobs", bucket), 0700)
		}
	}
	db, err := createOpen(db_path, opts())
	if err != nil {
		panic(err)
	}
	return &DiskLRU{f, threshold, db, path, sync.Mutex{}}, err
}

func NewTest(f func(string) []byte, threshold int64) (*DiskLRU, error) {
	db, err := kv.CreateMem(&kv.Options{})
	return &DiskLRU{f, threshold, db, "tmp_test_blobs_lru", sync.Mutex{}}, err
}

// Close cleanly close the kv db.
func (lru *DiskLRU) Close() {
	lru.db.Close()
}

func (lru *DiskLRU) Remove() {
	os.RemoveAll(lru.path)
}

// Build the key by adding the given byte to namespace keys in the DB.
func buildKey(ktype byte, key string) []byte {
	bkey := []byte(key)
	out := make([]byte, len(bkey)+1)
	out[0] = ktype
	copy(out[1:], bkey)
	return out
}

func indexKey(key string, index uint32) []byte {
	bkey := []byte(key)
	out := make([]byte, len(bkey)+5)
	out[0] = Index
	binary.BigEndian.PutUint32(out[1:5], index)
	copy(out[5:], bkey)
	return out
}

func decodeIndexKey(key []byte) (string, uint32) {
	return string(key[5:]), binary.BigEndian.Uint32(key[1:5])
}

// Get the value for the given key, call Func if the key isn't stored yet.
func (lru *DiskLRU) Get(key string) (data []byte, fetched bool, err error) {
	lru.Lock()
	defer func() {
		lru.Unlock()
		defer lru.evict()
	}()
	accessTime := time.Now().UTC().Unix()
	bkey := buildKey(Keys, key)
	lastAccessTime, err := lru.getUint32(bkey)
	if err != nil {
		return
	}
	if lastAccessTime == 0 {
		fetched = true
		data = lru.Func(key)
		if err = ioutil.WriteFile(filepath.Join(lru.path, "blobs", key[:2], key), data, 0644); err != nil {
			return
		}
		// Increments the internal counter (for size and items count)
		if err = lru.incrUint32([]byte{MetaSize}, len(data)); err != nil {
			return
		}
		if err = lru.incrUint32([]byte{MetaCnt}, 1); err != nil {
			return
		}
	} else {
		// Remove the precedent last access time
		data, err = ioutil.ReadFile(filepath.Join(lru.path, "blobs", key[:2], key))
		if err != nil {
			return
		}
		// Remove the precedent index entry
		if err = lru.db.Delete(indexKey(key, lastAccessTime)); err != nil {
			return
		}
	}
	// Update the last access time
	nikey := indexKey(key, uint32(accessTime))
	if err = lru.putUint32(nikey, uint32(len(data))); err != nil {
		return
	}
	// Update the Keys index access time
	if err = lru.putUint32(bkey, uint32(accessTime)); err != nil {
		return
	}
	return
}

// remove remove an time from the cache
// (it doesn't decrement internal counter about size/cnt).
func (lru *DiskLRU) remove(key string) (err error) {
	bkey := buildKey(Keys, key)
	lastAccessTime, err := lru.getUint32(bkey)
	if err != nil {
		return
	}
	if lastAccessTime != 0 {
		ikey := indexKey(key, lastAccessTime)
		if err = lru.db.Delete(ikey); err != nil {
			return
		}
		if err = lru.db.Delete(bkey); err != nil {
			return
		}
	}
	return nil
}

// Size returns the total size of data stored to disk.
func (lru *DiskLRU) Size() uint32 {
	lru.Lock()
	defer lru.Unlock()
	skey := []byte{MetaSize}
	tsize, _ := lru.getUint32(skey)
	return tsize
}

// Cnt returns the number of items in the cache.
func (lru *DiskLRU) Cnt() uint32 {
	lru.Lock()
	defer lru.Unlock()
	skey := []byte{MetaCnt}
	cnt, _ := lru.getUint32(skey)
	return cnt
}

// evict evicts least recently items until the total size is under the threshold.
func (lru *DiskLRU) evict() error {
	skey := []byte{MetaSize}
	tsize, err := lru.getUint32(skey)
	if err != nil {
		return err
	}
	if int64(tsize) > lru.Threshold {
		items := make(chan *CacheItem)
		go lru.Iter(items)
		csize := int64(tsize)
		for item := range items {
			// The cache must contains at list one element, even above the threshold
			if csize > lru.Threshold {
				if err := lru.remove(item.Key); err != nil {
					return err
				}
				lru.incrUint32([]byte{MetaSize}, -int(item.Size))
				lru.incrUint32([]byte{MetaCnt}, -1)
				csize -= int64(item.Size)
			}
		}
	}
	return nil
}

// Iter iterate over keys and index (unix timestamp of last access).
func (lru *DiskLRU) Iter(items chan<- *CacheItem) {
	lru.Lock()
	defer lru.Unlock()
	defer close(items)
	enum, _, _ := lru.db.Seek(buildKey(Index, ""))
	endBytes := buildKey(Index, "\xff")
	for {
		k, v, err := enum.Next()
		if err == io.EOF || bytes.Compare(k, endBytes) > 0 {
			break
		}
		rkey, rindex := decodeIndexKey(k)
		rsize := binary.LittleEndian.Uint32(v)
		items <- &CacheItem{Key: rkey, Index: rindex, Size: rsize}
	}
}

// Store a uint32 as binary data.
func (lru *DiskLRU) putUint32(key []byte, value uint32) error {
	val := make([]byte, 4)
	binary.LittleEndian.PutUint32(val[:], value)
	err := lru.db.Set(key, val)
	return err
}

// Retrieve a binary encoded uint32.
func (lru *DiskLRU) getUint32(key []byte) (uint32, error) {
	data, err := lru.db.Get(nil, key)
	if err != nil || data == nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(data), nil
}

// Increment a binary encoded uint32.
func (lru *DiskLRU) incrUint32(key []byte, step int) error {
	data, err := lru.db.Get(nil, key)
	var value uint32
	if err != nil {
		return err
	}
	if data == nil {
		value = 0
	} else {
		value = binary.LittleEndian.Uint32(data)
	}
	val := make([]byte, 4)
	binary.LittleEndian.PutUint32(val[:], value+uint32(step))
	err = lru.db.Set(key, val)
	return err
}
