/*

Package mirror implement the mirror backend designed to store blobs to multiple backends.

Get/Exists/Enumerate requests are performed on the first BlobHandler.

*/
package mirror

import (
	"github.com/tsileo/datadatabase/backend"
)

type MirrorBackend struct {
	backends []backend.BlobHandler
}

func New(backends ...backend.BlobHandler) *MirrorBackend {
	b := &MirrorBackend{[]backend.BlobHandler{}}
	for _, mBackend := range backends {
		b.backends = append(b.backends, mBackend)
	}
	return b
}

func (backend *MirrorBackend) Close() {
	for _, b := range backend.backends {
		b.Close()
	}
}
func (backend *MirrorBackend) Put(hash string, data []byte) (err error) {
	for _, b := range backend.backends {
		if err := b.Put(hash, data); err != nil {
			return err
		}
	}
	return
}


func (backend *MirrorBackend) Exists(hash string) bool {
	for _, b := range backend.backends {
		return b.Exists(hash)
	}
	return false
}

func (backend *MirrorBackend) Get(hash string) (data []byte, err error) {
	for _, b := range backend.backends {
		data, err = b.Get(hash)
		if err == nil {
			return
		}
	}
	return
}

func (backend *MirrorBackend) Enumerate(blobs chan<- string) error {
	// TODO(tsileo) enumerate over all backends with a map to check if already sent ?
	for _, b := range backend.backends {
		return b.Enumerate(blobs)
	}
	return nil
}
