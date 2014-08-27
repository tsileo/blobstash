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

type MirrorBackend struct {
	backends []backend.BlobHandler
}

func New(backends ...backend.BlobHandler) *MirrorBackend {
	log.Println("MirrorBackend: starting")
	b := &MirrorBackend{[]backend.BlobHandler{}}
	for _, mBackend := range backends {
		log.Printf("MirrorBackend: adding backend %v", mBackend.String())
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
	for _, b := range backen.backends {
		tblobs := make(chan string)
		err := b.Enumerate(tblobs)
		go func() {
			for bl := range tblobs {
				blobs <- bl
			}
		}()
		switch err {
		case backend.ErrWriteOnly:
			continue
		default:
			return err
		}
	}
	return fmt.Errorf("shouldn't happen")
}
