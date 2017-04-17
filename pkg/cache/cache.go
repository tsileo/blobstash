package cache

import (
	"bytes"
	"container/list"
	"encoding/binary"
	"io"
	"os"
	_ "path/filepath"
	"time"

	"github.com/cznic/kv"
)

type Cache struct {
	evict       *list.List
	items       map[string]*list.Element
	maxSize     int
	currentSize int

	db *kv.DB
}

// kv max value size
var limit = 65787

func split(buf []byte) [][]byte {
	var chunk []byte
	chunks := make([][]byte, 0, len(buf)/limit+1)
	for len(buf) >= limit {
		chunk, buf = buf[:limit], buf[limit:]
		chunks = append(chunks, chunk)
	}
	if len(buf) > 0 {
		chunks = append(chunks, buf[:len(buf)])
	}
	return chunks
}

type element struct {
	key        string
	size       int
	lastAccess int64
}

func New(maxSize int) (*Cache, error) {
	// TODO ensue maxSize > 0
	// path := filepath.Join(conf.VarDir(), indexName)
	path := "ok"
	createOpen := kv.Open
	if _, err := os.Stat(path); os.IsNotExist(err) {
		createOpen = kv.Create
	}
	db, err := createOpen(path, &kv.Options{})
	if err != nil {
		return nil, err
	}

	return &Cache{
		maxSize: maxSize,
		evict:   list.New(),
		items:   map[string]*list.Element{},
		db:      db,
	}, nil
}

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
	bkey := append([]byte(key), []byte(":")...)
	enum, ok, err := c.db.Seek(buildKey(bkey, 0))
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	i := 0
	for {
		k, _, err := enum.Next()
		if err == io.EOF || !bytes.Equal(k, buildKey(bkey, i)) {
			// fmt.Printf("eof\n")
			break
		}
		if err != nil {
			return err
		}
		// fmt.Printf("\n\n%+v\n%s\n%+v\n%s\n%v\n%d / %d\n====\n", k, k, bkey, bkey, bytes.HasPrefix(k, bkey), len(v), buf.Len())
		if !bytes.HasPrefix(k, bkey) {
			break
		}
		if err := c.db.Delete(k); err != nil {
			return err
		}
		i++
	}
	return nil
}

func (c *Cache) dbGet(key string) ([]byte, error) {
	var buf bytes.Buffer
	bkey := append([]byte(key), []byte(":")...)
	enum, ok, err := c.db.Seek(buildKey(bkey, 0))
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	for {
		k, v, err := enum.Next()
		if err == io.EOF {
			// fmt.Printf("eof\n")
			break
		}
		if err != nil {
			return nil, err
		}
		// fmt.Printf("\n\n%+v\n%s\n%+v\n%s\n%v\n%d / %d\n====\n", k, k, bkey, bkey, bytes.HasPrefix(k, bkey), len(v), buf.Len())
		if !bytes.HasPrefix(k, bkey) {
			break
		}
		buf.Write(v)
	}
	// fmt.Printf("freturn=%d", buf.Len())
	return buf.Bytes(), nil
}

func buildKey(bkey []byte, i int) []byte {
	k := make([]byte, len(bkey)+8)
	copy(k[:], bkey)
	binary.BigEndian.PutUint64(k[len(bkey):], uint64(i))
	return k
}

func (c *Cache) dbSet(key string, value []byte) error {
	bkey := append([]byte(key), []byte(":")...)
	chunks := split(value)
	// fmt.Printf("len value=%d\n", len(value))
	for i, chunk := range chunks {
		// fmt.Printf("chunk %d (len=%d)\n", i, len(chunk))
		k := buildKey(bkey, i)
		if err := c.db.Set(k, chunk); err != nil {
			return err
		}
	}
	return nil
}

func (c *Cache) Add(key string, value []byte) error {
	lastAccess := time.Now().UnixNano()
	ts := make([]byte, 8)
	binary.BigEndian.PutUint64(ts[:], uint64(lastAccess))
	// Check for existing item
	size := len(value)
	if elm, ok := c.items[key]; ok {
		c.evict.MoveToFront(elm)
		c.currentSize -= elm.Value.(*element).size
		c.currentSize += len(value)
		elm.Value.(*element).size = size
		elm.Value.(*element).lastAccess = lastAccess
		if err := c.dbSet(key, value); err != nil {
			return err
		}
		if err := c.db.Set([]byte("_key:"+key), ts); err != nil {
			return err
		}
		return c.doEviction()
	}

	// Add new item
	elm := &element{key, len(value), lastAccess}
	entry := c.evict.PushFront(elm)
	c.items[key] = entry
	c.currentSize += len(value)
	if err := c.dbSet(key, value); err != nil {
		return err
	}
	if err := c.db.Set([]byte("_key:"+key), ts); err != nil {
		return err
	}

	return c.doEviction()
}

func (c *Cache) doEviction() error {
	for c.currentSize > c.maxSize {
		elm := c.evict.Back()
		if elm != nil {
			entry := elm.Value.(*element)
			if err := c.db.Delete([]byte("_key:" + entry.key)); err != nil {
				return err
			}
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
