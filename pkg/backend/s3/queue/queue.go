// Package queue implements a basic FIFO queues, the data are ordered at second precision.
package queue // import "a4.io/blobstash/pkg/backend/s3/queue"

import (
	"encoding/json"
	"io"
	"os"
	"sync"
	"time"

	"github.com/cznic/kv"

	"a4.io/blobstash/pkg/docstore/id"
)

// Queue is a FIFO queue,
type Queue struct {
	db   *kv.DB
	path string
	sync.Mutex
}

// New creates a new database.
func New(path string) (*Queue, error) {
	createOpen := kv.Open
	if _, err := os.Stat(path); os.IsNotExist(err) {
		createOpen = kv.Create
	}

	kvdb, err := createOpen(path, &kv.Options{})
	if err != nil {
		return nil, err
	}

	return &Queue{
		db:   kvdb,
		path: path,
	}, nil
}

// Close the underlying db file.
func (q *Queue) Close() error {
	return q.db.Close()
}

// Remove the underlying db file.
func (q *Queue) Remove() error {
	return os.Remove(q.path)
}

// Enqueue the given `item`. Must be JSON serializable.
func (q *Queue) Enqueue(item interface{}) error {
	q.Lock()
	defer q.Unlock()

	id, err := id.New(time.Now().Unix())
	if err != nil {
		return err
	}

	js, err := json.Marshal(item)
	if err != nil {
		return err
	}

	q.db.Set(id.Raw(), js)

	return nil
}

// Dequeue the older item, unserialize the given item.
// Returns false if the queue is empty.
func (q *Queue) Dequeue(item interface{}) (bool, func(bool), error) {
	q.Lock()
	defer q.Unlock()

	enum, err := q.db.SeekFirst()
	if err != nil {
		if err == io.EOF {
			return false, nil, nil
		}
		return false, nil, err
	}

	k, v, err := enum.Next()
	if err != nil && err != io.EOF {
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
