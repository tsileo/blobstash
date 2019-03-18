/*

Package queue implements a basic FIFO queues.

*/
package queue // import "a4.io/blobstash/pkg/queue"

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/recoilme/pudge"

	"a4.io/blobstash/pkg/blob"
	"a4.io/blobstash/pkg/docstore/id"
)

// Queue is a FIFO queue,
type Queue struct {
	db   *pudge.Db
	path string
}

// New creates a new database.
func New(path string) (*Queue, error) {
	db, err := pudge.Open(path, nil)
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
	return q.db.DeleteFile()
}

// Size returns the number of items currently enqueued
func (q *Queue) Size() (int, error) {
	return q.db.Count()
}

func (q *Queue) Blobs() ([]*blob.Blob, error) {
	keys, err := q.db.Keys(nil, 0, 0, true)
	if err != nil {
		return nil, fmt.Errorf("failed to list keys: %v", err)
	}
	out := []*blob.Blob{}

	for _, k := range keys {
		b := &blob.Blob{}
		d := []byte{}
		if err := q.db.Get(k, &d); err != nil {
			return nil, err

		}
		if err := json.Unmarshal(d, b); err != nil {
			return nil, err
		}

		out = append(out, b)
	}
	return out, nil
}

// Enqueue the given `item`. Must be JSON serializable.
func (q *Queue) Enqueue(item interface{}) (*id.ID, error) {
	id, err := id.New(time.Now().Unix())
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
	keys, err := q.db.Keys(nil, 1, 0, true)
	if err != nil {
		return false, nil, err
	}
	if len(keys) == 0 {
		return false, nil, nil
	}
	k := keys[0]
	v := []byte{}
	if err := q.db.Get(k, &v); err != nil {
		fmt.Printf("failed to get %v\n", err)
		return false, nil, err
	}
	deqFunc := func(remove bool) {
		if !remove {
			return
		}

		if err := q.db.Delete(k); err != nil {
			panic(err)
		}
	}

	return true, deqFunc, json.Unmarshal(v, item)
}

// TODO(tsileo): func (q *Queue) Items() ([]*blob.Blob, error)
// also use `*blob.Blob` instead if `interface{}`
