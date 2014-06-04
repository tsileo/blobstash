/*

Package glacier implement a write-only backend backed by AWS Glacier that upload blobs incrementally,
it uses a blobsfile as cache to buffer blobs, and once Done is called (when a transaction is committed),
the blobsfile will be uploaded.


Put/Exists/Enumerate requests are performed on the cache.
Get is disabled since it's a write-only backend.

*/
package glacier

import (
	"expvar"
	"fmt"
	"log"
	_ "strings"

	"github.com/tsileo/datadatabase/backend"
)

var (
	bytesUploaded   = expvar.NewMap("glacier-bytes-uploaded")
	bytesDownloaded = expvar.NewMap("glacier-bytes-downloaded")
	blobsUploaded   = expvar.NewMap("glacier-blobs-uploaded")
	blobsDownloaded = expvar.NewMap("glacier-blobs-downloaded")
)

type GlacierBackend struct {
	cache backend.BlobHandler
}

func New(cache backend.BlobHandler) *GlacierBackend {
	log.Println("GlacierBackend: starting")
	return &GlacierBackend{cache}
}

func (backend *GlacierBackend) String() string {
	// TODO add the vault
	return fmt.Sprintf("glacier-")
}

func (backend *GlacierBackend) Close() {
	backend.cache.Close()
}

func (backend *GlacierBackend) Done() error {
	// TODO handle upload to Glacier
	if err := backend.cache.Done(); err != nil {
		return err
	}
	return nil
}

// TODO a way to restore

func (backend *GlacierBackend) Put(hash string, data []byte) (err error) {
	if err := backend.cache.Put(hash, data); err != nil {
		return err
	}
	bytesUploaded.Add("total", int64(len(data)))
	blobsUploaded.Add("total", 1)
	bytesUploaded.Add(backend.String(), int64(len(data)))
	blobsUploaded.Add(backend.String(), 1)
	return
}

func (backend *GlacierBackend) Exists(hash string) bool {
	return backend.cache.Exists(hash)
}

func (backend *GlacierBackend) Get(hash string) (data []byte, err error) {
	panic("GlacierBackend is a write-only backend")
	return
}

func (backend *GlacierBackend) Enumerate(blobs chan<- string) error {
	// TODO(tsileo) enumerate over all backends with a map to check if already sent ?
	return backend.cache.Enumerate(blobs)
}
