package backend

import (
	"os"
	"log"
	"syscall"
	"fmt"
	"path/filepath"
)

type BlobsFileBackend struct {
	Directory string

	// Current blobs file opened for write
	current *os.File
	// Size of the current blobs file
	size int64
	// Old blobs files opened for read
	files []*os.File

	sync.Mutex
}

func NewBlobsFileBackend(dir string) *LocalBackend {
	os.Mkdir(dir, 0744)
	return &BlobsFileBackend{dir}
}

// Generate a new blobs file and fallocate a 256MB file.
func (backend *BlobsFileBackend) allocateBlobsFile(n int) error {
	f, err := os.OpenFile(filepath.Join(backend.Directory, fmt.Sprintf("/box/blobs-%05d", n)),
		os.O_RDWR|os.O_CREATE, 0666)
	defer f.Close()
	if err != nil {
		return err
	}
	// fallocate 256MB
	if err := syscall.Fallocate(int(f.Fd()), 0x01, 0, 256 << 20); err != nil {
		return err
	}
}
