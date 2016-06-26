package backend

import (
	"errors"
)

var ErrWriteOnly = errors.New("backend is write-only")

type Config interface {
	Config() map[string]interface{}
	Map() map[string]interface{}
	Backend() string
}

// BlobHandler is the interface that defines
// all the method a "blob backend" must implement.
type BlobHandler2 interface {
	Put(hash string, data []byte) error
	Exists(hash string) (bool, error)
	// Delete(hash string) error
	Get(hash string) (data []byte, err error)
	Enumerate2(start, end string, limit int) error
	Close()
	// Done is called when a transaction finished
	// Done() error
	String() string
}

// BlobHandler is the interface that defines
// all the method a "blob backend" must implement.
type BlobHandler interface {
	Put(hash string, data []byte) error
	Exists(hash string) (bool, error)
	Delete(hash string) error
	Get(hash string) (data []byte, err error)
	Enumerate(chan<- string) error
	Close()
	// Done is called when a transaction finished
	Done() error
	String() string
}
