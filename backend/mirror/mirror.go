/*

Package mirror implement the mirror backend designed to store blobs to multiple backends.

Get/Exists/Enumerate requests are performed on the first BlobHandler.

*/
package mirror

import (
	"expvar"
	"fmt"
	"log"
	"strings"

	"github.com/tsileo/blobstash/backend"
)

var (
	bytesUploaded   = expvar.NewMap("mirror-bytes-uploaded")
	bytesDownloaded = expvar.NewMap("mirror-bytes-downloaded")
	blobsUploaded   = expvar.NewMap("mirror-blobs-uploaded")
	blobsDownloaded = expvar.NewMap("mirror-blobs-downloaded")
)

type Config struct {
	WriteBackends []backend.Config `structs:"write-backends,omitempty"`
	Backends      []backend.Config `structs:"backends,omitempty"`
}

func (c *Config) Backend() string {
	return "mirror"
}

func (c *Config) Config() map[string]interface{} {
	return map[string]interface{}{
		"backend-type": c.Backend(),
		"backend-args": c.Map(),
	}
}

func (c *Config) Map() map[string]interface{} {
	return nil
}

type MirrorBackend struct {
	backends          []backend.BlobHandler
	readWriteBackends []backend.BlobHandler
	writeBackends     []backend.BlobHandler
}

func New(rwbackends []backend.BlobHandler, wbackends []backend.BlobHandler) *MirrorBackend {
	log.Println("MirrorBackend: starting")
	b := &MirrorBackend{
		backends:          []backend.BlobHandler{},
		readWriteBackends: []backend.BlobHandler{},
		writeBackends:     []backend.BlobHandler{},
	}
	for _, mBackend := range rwbackends {
		log.Printf("MirrorBackend: adding Write/Read backend %v", mBackend.String())
		b.readWriteBackends = append(b.readWriteBackends, mBackend)
		b.backends = append(b.backends, mBackend)
	}
	for _, mBackend := range wbackends {
		log.Printf("MirrorBackend: adding Write-only backend %v", mBackend.String())
		b.writeBackends = append(b.writeBackends, mBackend)
		b.backends = append(b.backends, mBackend)
	}
	return b
}

func (backend *MirrorBackend) String() string {
	backends := []string{}
	for _, b := range backend.backends {
		backends = append(backends, b.String())
	}
	return fmt.Sprintf("mirror-%v", strings.Join(backends, "-"))
}

func (backend *MirrorBackend) Close() {
	for _, b := range backend.backends {
		b.Close()
	}
}

func (backend *MirrorBackend) Done() error {
	for _, b := range backend.backends {
		if err := b.Done(); err != nil {
			return err
		}
	}
	return nil
}

func (backend *MirrorBackend) Put(hash string, data []byte) (err error) {
	for _, b := range backend.backends {
		if err := b.Put(hash, data); err != nil {
			return err
		}
		bytesUploaded.Add("total", int64(len(data)))
		blobsUploaded.Add("total", 1)
		bytesUploaded.Add(b.String(), int64(len(data)))
		blobsUploaded.Add(b.String(), 1)
	}
	return
}

func (backend *MirrorBackend) Exists(hash string) (bool, error) {
	for _, b := range backend.readWriteBackends {
		return b.Exists(hash)
	}
	return false, nil
}

func (backend *MirrorBackend) Delete(hash string) error {
	for _, b := range backend.backends {
		if err := b.Delete(hash); err != nil {
			return err
		}
	}
	return nil
}

func (backend *MirrorBackend) Get(hash string) (data []byte, err error) {
	for _, b := range backend.readWriteBackends {
		data, err = b.Get(hash)
		if err == nil {
			blobsDownloaded.Add("total", 1)
			bytesDownloaded.Add("total", int64(len(data)))
			blobsDownloaded.Add(b.String(), 1)
			bytesDownloaded.Add(b.String(), int64(len(data)))
			return
		} else {
			log.Printf("MirrorBackend: error fetching blob %v from backend %b", hash, b.String())
		}
	}
	return
}

func (backen *MirrorBackend) Enumerate(blobs chan<- string) error {
	defer close(blobs)
	for _, b := range backen.readWriteBackends {
		errc := make(chan error)
		tblobs := make(chan string)
		go func() {
			errc <- b.Enumerate(tblobs)
		}()
		for bl := range tblobs {
			blobs <- bl
		}
		err := <-errc
		switch err {
		case backend.ErrWriteOnly:
			continue
		default:
			return err
		}
	}
	return fmt.Errorf("shouldn't happen")
}
