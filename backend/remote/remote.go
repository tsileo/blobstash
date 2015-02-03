package remote

import (
	"fmt"

	"github.com/tsileo/blobstash/client"
)

type RemoteBackend struct {
	bs   *client.BlobStore
	addr string
}

func New(addr string) *RemoteBackend {
	return &RemoteBackend{
		bs:   client.NewBlobStore(addr),
		addr: addr,
	}
}

func (backend *RemoteBackend) String() string {
	return fmt.Sprintf("remote-%v", backend.addr)
}

func (backend *RemoteBackend) Close() {
	return
}

func (backend *RemoteBackend) Put(hash string, data []byte) (err error) {
	return backend.bs.Put(hash, data)
}

func (backend *RemoteBackend) Exists(hash string) (bool, error) {
	return backend.bs.Stat(hash)
}

func (backend *RemoteBackend) Delete(hash string) error {
	return backend.bs.Delete(hash)
}

func (backend *RemoteBackend) Get(hash string) (data []byte, err error) {
	return backend.bs.Get(hash)
}

func (backend *RemoteBackend) Enumerate(blobs chan<- string) error {
	return backend.bs.Enumerate(blobs, "", "\xff", 0)
}
