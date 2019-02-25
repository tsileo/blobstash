// Package cache implements a disk-backed LRU cache for "big" binary blob
package cache // import "a4.io/blobstash/pkg/cache"

import (
	"container/list"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// Cache holds a cache instance, backed by a single file and an in-memory list
type Cache struct {
	evict       *list.List
	items       map[string]*list.Element
	maxSize     int64
	currentSize int64
	path        string
}

type element struct {
	key        string
	size       int64
	lastAccess int64
}

// New initializes a new LRU cache
func New(dir, name string, maxSize int64) (*Cache, error) {
	if !(maxSize > 0) {
		return nil, fmt.Errorf("maxSize should be greater than 0")
	}
	path := filepath.Join(dir, name)

	if _, err := os.Stat(path); os.IsNotExist(err) {
		if err := os.MkdirAll(path, 0700); err != nil {
			return nil, err
		}
	}
	cache := &Cache{
		maxSize: maxSize,
		evict:   list.New(),
		items:   map[string]*list.Element{},
		path:    path,
	}
	files, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, err
	}
	elements := []*element{}

	for _, file := range files {
		e := &element{
			key:        file.Name(),
			lastAccess: int64(file.ModTime().Unix()),
			size:       file.Size(),
		}

		elements = append(elements, e)
	}
	sort.Slice(elements, func(i, j int) bool { return elements[i].lastAccess < elements[j].lastAccess })
	for _, e := range elements {
		entry := cache.evict.PushFront(e)
		cache.items[e.key] = entry
		cache.currentSize += e.size
	}

	return cache, nil
}

// Close closes the cache
func (c *Cache) Close() error {
	return nil
}

// Stat returns true if the key is stored
func (c *Cache) Stat(key string) (bool, error) {
	if _, err := os.Stat(filepath.Join(c.path, key)); os.IsNotExist(err) {
		return false, nil
	}
	return true, nil
}

// Get returns the given key if present
func (c *Cache) Get(key string) ([]byte, bool, error) {
	if elm, ok := c.items[key]; ok {
		c.evict.MoveToFront(elm)
		data, err := c.dbGet(key)
		if err != nil || data == nil {
			return nil, false, err
		}
		return data, true, nil
	}
	return nil, false, nil
}

func (c *Cache) dbDelete(key string) error {
	return os.Remove(filepath.Join(c.path, key))
}

func (c *Cache) dbGet(key string) ([]byte, error) {
	dat, err := ioutil.ReadFile(filepath.Join(c.path, key))
	if err != nil {
		return nil, err
	}

	return dat, nil
}

// Add adds/updates the given key/value pair
func (c *Cache) Add(key string, value []byte) error {
	lastAccess := time.Now().UnixNano()
	// Check for existing item
	size := int64(len(value))
	if elm, ok := c.items[key]; ok {
		c.evict.MoveToFront(elm)
		c.currentSize += size - elm.Value.(*element).size
		elm.Value.(*element).size = size
		elm.Value.(*element).lastAccess = lastAccess
		return c.doEviction()
	}

	// Add new item
	elm := &element{key, size, lastAccess}
	entry := c.evict.PushFront(elm)
	c.items[key] = entry
	c.currentSize += size
	if err := ioutil.WriteFile(filepath.Join(c.path, key), value, 0600); err != nil {
		return err
	}

	return c.doEviction()
}

func (c *Cache) doEviction() error {
	for c.currentSize > c.maxSize {
		elm := c.evict.Back()
		if elm != nil {
			entry := elm.Value.(*element)
			if err := c.dbDelete(entry.key); err != nil {
				return err
			}
			c.currentSize -= entry.size
			c.evict.Remove(elm)
			delete(c.items, entry.key)
		}
	}
	return nil
}

// Len returns the number of items stored
func (c *Cache) Len() int {
	return len(c.items)
}

// Size returns the disk usage of the cache file
func (c *Cache) Size() int64 {
	return c.currentSize
}
