package blobsfile

import (
	"os"
	"syscall"
	"sync"
	"fmt"
	"path/filepath"
	"log"
)

type BlobsFileBackend struct {
	Directory string

	index *BlobsIndex

	// Current blobs file opened for write
	n int
	current *os.File
	// Size of the current blobs file
	size int64
	// Old blobs files opened for read
	files map[int]*os.File

	sync.Mutex
}

func NewBlobsFileBackend(dir string) *BlobsFileBackend {
	os.Mkdir(dir, 0744)
	index, _ := NewIndex(dir)
	return &BlobsFileBackend{Directory: dir, index: index, files: make(map[int]*os.File)}
}

func (backend *BlobsFileBackend) Close() {
	backend.index.Close()
}

func (backend *BlobsFileBackend) Remove() {
	backend.index.Remove()
}

// Open all the blobs-XXXXX (read-only) and open the last for write
func (backend *BlobsFileBackend) load() error {
	n := 0
	for {
		err := backend.ropen(n)
		if os.IsNotExist(err) {
			break
		}
		if err != nil {
			return err
		}
		n++
	}
	if n == 0 {
		// The dir is empty, create a new blobs-XXXXX file,
		// and open it for read
		if err := backend.wopen(n); err != nil {
			return err
		}
		if err := backend.ropen(n); err != nil {
			return err
		}
		return nil
	}
	// Open the last file for write
	if err := backend.wopen(n - 1); err != nil {
		return err
	}
	return nil
}

// Open a file for write
func (backend *BlobsFileBackend) wopen(n int) error {
	// Close the already opened file if any
	if backend.current != nil {
		if err := backend.current.Close(); err != nil {
			return err
		}
	}
	fallocate := false
	if _, err := os.Stat(backend.filename(n)); os.IsNotExist(err) {
		fallocate = true
	}
	f, err := os.OpenFile(backend.filename(n), os.O_CREATE|os.O_RDWR, 0666)
	defer f.Close()
	if err != nil {
		return err
	}
	backend.current = f
	if fallocate {
		if ferr := backend.allocateBlobsFile(); ferr != nil {
			log.Printf("Error fallocate file %v: %v", backend.filename(n), ferr)
		}
	}
	backend.size, err = f.Seek(0, os.SEEK_END)
	log.Printf("Seeked %v at %v", n, backend.size)
	if err != nil {
		return err
	}
	return nil
}

// Open a file for read
func (backend *BlobsFileBackend) ropen(n int) error {
	backend.Lock()
	defer backend.Unlock()
	_, alreadyOpen := backend.files[n]
	if alreadyOpen {
		return fmt.Errorf("File %v already open", n)
	}
	if n > len(backend.files) {
		return fmt.Errorf("Trying to open file %v whereas only %v files currently open", n, len(backend.files))
	}

	filename := backend.filename(n)
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	backend.files[n] = f
	return nil
}

// Generate a new blobs file and fallocate a 256MB file.
func (backend *BlobsFileBackend) allocateBlobsFile() error {
	// fallocate 256MB
	if err := syscall.Fallocate(int(backend.current.Fd()), 0x01, 0, 256 << 20); err != nil {
		return err
	}
	return nil
}

func (backend *BlobsFileBackend) filename(n int) string {
	return filepath.Join(backend.Directory, fmt.Sprintf("blobs-%05d", n))
}

