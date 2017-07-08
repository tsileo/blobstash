package rangedb // import "a4.io/blobstash/pkg/rangedb"

import (
	"bytes"
	"io"
	"os"
	"sync"

	"github.com/cznic/kv"
)

type RangeDB struct {
	db   *kv.DB
	path string
	mu   *sync.Mutex
}

// New creates a new database.
func New(path string) (*RangeDB, error) {
	createOpen := kv.Open
	if _, err := os.Stat(path); os.IsNotExist(err) {
		createOpen = kv.Create
	}
	kvdb, err := createOpen(path, &kv.Options{})
	if err != nil {
		return nil, err
	}
	return &RangeDB{
		db:   kvdb,
		path: path,
		mu:   new(sync.Mutex),
	}, nil
}

func (db *RangeDB) Close() error {
	return db.db.Close()
}

func (db *RangeDB) Destroy() error {
	if db.path != "" {
		db.Close()
		return os.RemoveAll(db.path)
	}
	return nil
}

func (db *RangeDB) Set(k, v []byte) error {
	if err := db.db.Set(k, v); err != nil {
		return err
	}
	return nil
}

func (db *RangeDB) Get(k []byte) ([]byte, error) {
	return db.db.Get(nil, k)
}

type Range struct {
	Reverse  bool
	Min, Max []byte
	db       *RangeDB
	enum     *kv.Enumerator
}

func (db *RangeDB) Range(min, max []byte, reverse bool) *Range {
	return &Range{
		Min:     min,
		Max:     max,
		Reverse: reverse,
		db:      db,
	}
}

func (c *Range) first() ([]byte, []byte, error) {
	var err error
	if c.Reverse {
		c.enum, _, err = c.db.db.Seek(c.Max)
		if err != nil {
			return nil, nil, err
		}
		k, v, err := c.enum.Prev()
		if err == io.EOF {
			c.enum, err = c.db.db.SeekLast()
			return c.enum.Prev()
		}
		if err != nil {
			return nil, nil, err
		}
		if bytes.Compare(k, c.Max) > 0 {
			k, v, err = c.enum.Prev()
		}
		return k, v, err
	}

	c.enum, _, err = c.db.db.Seek(c.Min)
	return c.enum.Next()
}

func (c *Range) next() ([]byte, []byte, error) {
	if c.enum == nil {
		return c.first()
	}
	if c.Reverse {
		return c.enum.Prev()
	}
	return c.enum.Next()
}

func (c *Range) Next() ([]byte, []byte, error) {
	k, v, err := c.next()
	if c.shouldContinue(k) {
		return k, v, err
	}
	return nil, nil, io.EOF
}

func (c *Range) shouldContinue(key []byte) bool {
	if c.Reverse {
		return key != nil && bytes.Compare(key, c.Min) >= 0
	}

	return key != nil && bytes.Compare(key, c.Max) <= 0
}
