package rangedb // import "a4.io/blobstash/pkg/rangedb"

import (
	"io"
	"os"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type RangeDB struct {
	db   *leveldb.DB
	path string
}

// New creates a new database.
func New(path string) (*RangeDB, error) {
	var err error
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, err
	}
	return &RangeDB{
		db:   db,
		path: path,
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
	return db.db.Put(k, v, nil)
}

func (db *RangeDB) Get(k []byte) ([]byte, error) {
	v, err := db.db.Get(k, nil)
	if err != nil {
		if err == errors.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	return v, nil
}

func (db *RangeDB) Has(k []byte) (bool, error) {
	e, err := db.db.Has(k, nil)
	if err != nil {
		return false, err
	}
	return e, nil
}

// NextKey returns the next key for lexigraphical (key = NextKey(lastkey))
func NextKey(bkey []byte) []byte {
	i := len(bkey)
	for i > 0 {
		i--
		bkey[i]++
		if bkey[i] != 0 {
			break
		}
	}
	return bkey
}

type Range struct {
	Reverse  bool
	Min, Max []byte
	db       *RangeDB
	it       iterator.Iterator
	first    bool
}

func (db *RangeDB) PrefixRange(prefix []byte, reverse bool) *Range {
	iter := db.db.NewIterator(util.BytesPrefix(prefix), nil)
	return &Range{
		it:      iter,
		Reverse: reverse,
		db:      db,
		first:   true,
	}
}

func (db *RangeDB) Range(min, max []byte, reverse bool) *Range {
	iter := db.db.NewIterator(&util.Range{Start: min, Limit: NextKey(max)}, nil)
	return &Range{
		it:      iter,
		Min:     min,
		Max:     max,
		Reverse: reverse,
		db:      db,
		first:   true,
	}
}

func buildKv(it iterator.Iterator) ([]byte, []byte, error) {
	k := make([]byte, len(it.Key()))
	copy(k[:], it.Key())
	v := make([]byte, len(it.Value()))
	copy(v[:], it.Value())
	return k, v, nil
}

func (r *Range) Seek(k []byte) ([]byte, []byte, error) {
	if r.it.Seek(k) {
		return buildKv(r.it)
	}
	return nil, nil, io.EOF
}

func (r *Range) Next() ([]byte, []byte, error) {
	if !r.Reverse {
		if r.it.Next() {
			return buildKv(r.it)
		}

	} else {
		if r.first {
			if r.it.Last() {
				r.first = false
				return buildKv(r.it)
			}
		} else {
			if r.it.Prev() {
				return buildKv(r.it)
			}
		}
	}
	r.it.Release()
	if err := r.it.Error(); err != nil {
		return nil, nil, err
	}
	return nil, nil, io.EOF
}

func (r *Range) Close() error {
	r.it.Release()
	return r.it.Error()
}
