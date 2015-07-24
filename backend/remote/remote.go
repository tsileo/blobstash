package remote

import (
	"fmt"

	"github.com/tsileo/blobstash/backend"
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

func (b *RemoteBackend) String() string {
	return fmt.Sprintf("remote-%v", b.addr)
}

func (b *RemoteBackend) Close() {
	return
}

func (b *RemoteBackend) Done() error {
	return nil
}

func (b *RemoteBackend) Put(hash string, data []byte) (err error) {
	return b.bs.Put(hash, data)
}

func (b *RemoteBackend) Exists(hash string) (bool, error) {
	return b.bs.Stat(hash)
}

func (b *RemoteBackend) Delete(hash string) error {
	return backend.ErrWriteOnly
}

func (b *RemoteBackend) Get(hash string) (data []byte, err error) {
	return b.bs.Get(hash)
}

func (b *RemoteBackend) Enumerate(blobs chan<- string) error {
	return b.bs.Enumerate(blobs, "", "\xff", 0)
}
