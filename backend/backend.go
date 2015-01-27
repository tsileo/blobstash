package backend

import (
	"errors"
)

var ErrWriteOnly = errors.New("backend is write-only")

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
