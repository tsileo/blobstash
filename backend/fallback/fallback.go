/*

Package fallback implements a backend with a local blobsfile backend as fallback if the primary backend fails.

Failed upload will be retried periodically.

*/
package fallback

import (
	"fmt"
	"log"
	"time"

	"github.com/tsileo/blobstash/backend"
	"github.com/tsileo/blobstash/backend/blobsfile"
)

type FallbackBackend struct {
	Backend  backend.BlobHandler
	Fallback backend.BlobHandler
}

func New(b backend.BlobHandler, cacheDir string) *FallbackBackend {
	fb := &FallbackBackend{
		Backend:  b,
		Fallback: blobsfile.New(cacheDir, 0, false, false),
	}
	go fb.CatchUp()
	return fb
}

func (backend *FallbackBackend) CatchUp() {
Loop:
	for {
		time.Sleep(10 * time.Second)
		blobs := make(chan string)
		errc := make(chan error, 1)
		go func() {
			errc <- backend.Fallback.Enumerate(blobs)
		}()
		for h := range blobs {
			blob, err := backend.Fallback.Get(h)
			if err != nil {
				panic(err)
			}
			if err := backend.Backend.Put(h, blob); err != nil {
				// if there is an error, just wait for another 10 seconds
				continue Loop
			}
			// no error, delete the blob from fallback backend
			if err := backend.Fallback.Delete(h); err != nil {
				panic(err)
			}
			log.Printf("Fallback/CatchUp: blob %v sucessfully reuploaded to %v", h, backend.Backend)
		}
		if err := <-errc; err != nil {
			panic(err)
		}
	}
}

func (backend *FallbackBackend) String() string {
	return fmt.Sprintf("%v-fallback-%v", backend.Backend, backend.Fallback)
}

func (backend *FallbackBackend) Get(hash string) ([]byte, error) {
	return backend.Backend.Get(hash)
}

func (backend *FallbackBackend) Delete(hash string) error {
	return backend.Backend.Delete(hash)
}

func (backend *FallbackBackend) Enumerate(blobs chan<- string) error {
	return backend.Backend.Enumerate(blobs)
}

func (backend *FallbackBackend) Exists(hash string) (bool, error) {
	return backend.Backend.Exists(hash)
}

func (backend *FallbackBackend) Put(hash string, data []byte) (err error) {
	err := backend.Backend.Put(hash, data)
	if err == nil {
		return nil
	}
	log.Printf("Fallback: write to %v failed, fallback to %v", backend.Backend, backend.Fallback)
	return backend.Fallback.Put(hash, data)
}
