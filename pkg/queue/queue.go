/*

Package queue implements a basic FIFO queues.

*/
package queue // import "a4.io/blobstash/pkg/queue"

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"a4.io/blobstash/pkg/blob"
	"a4.io/blobstash/pkg/docstore/id"
	"a4.io/blobstash/pkg/rangedb"
)

// Queue is a FIFO queue,
type Queue struct {
	db   *rangedb.RangeDB
	path string
	sync.Mutex
}

// New creates a new database.
func New(path string) (*Queue, error) {
	db, err := rangedb.New(path)
	if err != nil {
		return nil, err
	}

	return &Queue{
		db:   db,
		path: path,
	}, nil
}

// Close the underlying db file.
func (q *Queue) Close() error {
	return q.db.Close()
}

// Remove the underlying db file.
func (q *Queue) Remove() error {
	return q.db.Destroy()
}

// Size returns the number of items currently enqueued
func (q *Queue) Size() (int, error) {
	cnt := 0
	c := q.db.PrefixRange([]byte(""), false)
	defer c.Close()

	// Iterate the range
	c.Next()
	var err error
	for ; err == nil; _, _, err = c.Next() {
		cnt++
	}
	return cnt, nil
}

func (q *Queue) RemoveBlobs(blobs []string) error {
	idx := map[string]struct{}{}
	for _, h := range blobs {
		idx[h] = struct{}{}
	}
	c := q.db.PrefixRange([]byte(""), false)
	defer c.Close()

	// Iterate the range
	k, v, err := c.Next()
	for ; err == nil; k, v, err = c.Next() {
		b := &blob.Blob{}
		if err := json.Unmarshal(v, b); err != nil {
			return err
		}
		if _, ok := idx[b.Hash]; ok {
			if err := q.db.Delete(k); err != nil {
				return err
			}
		}
	}
	if err == io.EOF {
		return nil
	}
	return err
}

func (q *Queue) Blobs() ([]*blob.Blob, error) {
	out := []*blob.Blob{}

	c := q.db.PrefixRange([]byte(""), false)
	defer c.Close()

	// Iterate the range
	_, v, err := c.Next()
	for ; err == nil; _, v, err = c.Next() {
		b := &blob.Blob{}
		if err := json.Unmarshal(v, b); err != nil {
			return nil, err
		}

		out = append(out, b)
	}
	return out, nil
}

// Enqueue the given `item`. Must be JSON serializable.
func (q *Queue) Enqueue(item interface{}) (*id.ID, error) {
	id, err := id.New(time.Now().UnixNano())
	if err != nil {
		return nil, err
	}

	js, err := json.Marshal(item)
	if err != nil {
		return nil, err
	}

	q.db.Set(id.Raw(), js)

	return id, nil
}

// InstantDequeue remove the given ID from the queue directly
func (q *Queue) InstantDequeue(id *id.ID) error {
	return q.db.Delete(id.Raw())
}

// Dequeue the older item, unserialize the given item.
// Returns false if the queue is empty.
func (q *Queue) Dequeue(item interface{}) (bool, func(bool), error) {
	c := q.db.PrefixRange([]byte(""), false)
	defer c.Close()

	// Iterate the range
	k, js, err := c.Next()
	if err != nil && err != io.EOF {
		return false, nil, fmt.Errorf("next failed: %v", err)
	}

	if js == nil || len(js) == 0 {
		return false, nil, nil
	}

	deqFunc := func(remove bool) {
		if !remove {
			return
		}

		if err := q.db.Delete(k); err != nil {
			panic(err)
		}
	}

	return true, deqFunc, json.Unmarshal(js, item)
}

// TODO(tsileo): func (q *Queue) Items() ([]*blob.Blob, error)
// also use `*blob.Blob` instead if `interface{}`
