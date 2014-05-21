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
	// All blobs files opened for read
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
	f, err := os.OpenFile(backend.filename(n), os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	backend.current = f
	backend.n = n
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

func (backend *BlobsFileBackend) Put(hash string, data []byte) (err error) {
	backend.Lock()
	defer backend.Unlock()
	size := len(data)
	// TODO(tsileo) use transactions
	// TODO(tsileo) => set head HASH size binary encoded
	blobPos := BlobPos{n: backend.n, offset: int(backend.size), size: int(size)}
	if err := backend.index.SetPos(hash, blobPos); err != nil {
		return err
	}
	n, err := backend.current.Write(data)
	backend.size += int64(n)
	if err != nil || n != size {
		return fmt.Errorf("Error writing blob (%v,%v,%v)", err, n , size)
	}
	return
}

func (backend *BlobsFileBackend) Exists(hash string) bool {
	blobPos, _ := backend.index.GetPos(hash)
	if blobPos != nil {
		return true
	}
	return false
}

func parseBlob(data []byte) (hash string, size int, blob []byte) {
	
	return
}

func encodeBlob(hash string, size int, blob []byte) (data []byte) {

}

func (backend *BlobsFileBackend) Get(hash string) ([]byte, error) {
	blobPos, err := backend.index.GetPos(hash)
	if err != nil {
		return nil, err
	}
	if blobPos == nil {
		return nil, fmt.Errorf("Blob %v not found in index", err)
	}
	data := make([]byte, blobPos.size)
	n, err := backend.files[blobPos.n].ReadAt(data, int64(blobPos.offset))
	if err != nil {
		return nil, err
	}
	if n != blobPos.size {
		return nil, fmt.Errorf("Bad blob %v size, got %v, expected %v", hash, n, blobPos.size)
	}
	return data, nil
}
