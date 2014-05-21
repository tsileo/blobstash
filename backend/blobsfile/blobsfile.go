package blobsfile

import (
	"os"
	"syscall"
	"sync"
	"fmt"
	"io"
	"path/filepath"
	"log"
	"encoding/binary"
)

const maxBlobsFileSize = 256 << 20; // 256MB

type BlobsFileBackend struct {
	Directory string

	loaded bool

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

func New(dir string) *BlobsFileBackend {
	log.Println("BlobsFileBackend: opening index")
	os.Mkdir(dir, 0744)
	index, _ := NewIndex(dir)
	backend := &BlobsFileBackend{Directory: dir, index: index, files: make(map[int]*os.File)}
	backend.load()
	return backend
}

func (backend *BlobsFileBackend) Close() {
	log.Println("BlobsFileBackend: closing index")
	backend.index.Close()
}

func (backend *BlobsFileBackend) Remove() {
	backend.index.Remove()
}

// Open all the blobs-XXXXX (read-only) and open the last for write
func (backend *BlobsFileBackend) load() error {
	log.Printf("BlobsFileBackend: scanning BlobsFiles...")
	n := 0
	for {
		err := backend.ropen(n)
		if os.IsNotExist(err) {
			break
		}
		if err != nil {
			return err
		}
		log.Printf("BlobsFileBackend: %v loaded", backend.filename(n))
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
		backend.loaded = true
		return nil
	}
	// Open the last file for write
	if err := backend.wopen(n - 1); err != nil {
		return err
	}
	backend.loaded = true
	return nil
}

// Open a file for write
func (backend *BlobsFileBackend) wopen(n int) error {
	log.Printf("BlobsFileBackend: opening %v for writing", backend.filename(n))
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
	if err != nil {
		return err
	}
	return nil
}

// Open a file for read
func (backend *BlobsFileBackend) ropen(n int) error {
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
	log.Printf("BlobsFileBackend: running fallocate on BlobsFile %v", backend.filename(backend.n))
	// fallocate 256MB
	if err := syscall.Fallocate(int(backend.current.Fd()), 0x01, 0, maxBlobsFileSize); err != nil {
		return err
	}
	return nil
}

func (backend *BlobsFileBackend) filename(n int) string {
	return filepath.Join(backend.Directory, fmt.Sprintf("blobs-%05d", n))
}

func (backend *BlobsFileBackend) Put(hash string, data []byte) (err error) {
	if !backend.loaded {
		panic("backend BlobsFileBackend not loaded")
	}
	backend.Lock()
	defer backend.Unlock()
	size := len(data)
	blobPos := BlobPos{n: backend.n, offset: int(backend.size), size: int(size)}
	if err := backend.index.SetPos(hash, blobPos); err != nil {
		return err
	}
	blobEncoded := encodeBlob(size, data)
	n, err := backend.current.Write(blobEncoded)
	backend.size += int64(n + 4)
	if err != nil || n != size + 4 {
		return fmt.Errorf("Error writing blob (%v,%v,%v)", err, n, size)
	}
	if backend.size > maxBlobsFileSize {
		backend.n++
		log.Printf("BlobsFileBackend: creating a new BlobsFile")
		if err := backend.wopen(backend.n); err != nil {
			panic(err)
		}
		if err := backend.ropen(backend.n); err != nil {
			panic(err)
		}
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

func decodeBlob(data []byte) (size int, blob []byte) {
	size = int(binary.LittleEndian.Uint32(data[0:4]))
	return size, data[4:]
}

func encodeBlob(size int, blob []byte) (data []byte) {
	data = make([]byte, len(blob) + 4)
	binary.LittleEndian.PutUint32(data[:], uint32(size))
	copy(data[4:], blob)
	return data
}

func (backend *BlobsFileBackend) Get(hash string) ([]byte, error) {
	if !backend.loaded {
		panic("backend BlobsFileBackend not loaded")
	}
	blobPos, err := backend.index.GetPos(hash)
	if err != nil {
		return nil, err
	}
	if blobPos == nil {
		return nil, fmt.Errorf("Blob %v not found in index", err)
	}
	data := make([]byte, blobPos.size + 4)
	n, err := backend.files[blobPos.n].ReadAt(data, int64(blobPos.offset))
	if err != nil {
		return nil, err
	}
	if n != blobPos.size + 4 {
		return nil, fmt.Errorf("Error reading blob %v, read %v, expected %v", hash, n, blobPos.size)
	}
	blobSize, blob := decodeBlob(data)
	if blobSize != blobPos.size {
		return nil, fmt.Errorf("Bad blob %v encoded size, got %v, expected %v", hash, n , blobSize)
	}
	return blob, nil
}

func (backend *BlobsFileBackend) Enumerate(blobs chan<- string) error {
	backend.Lock()
	defer backend.Unlock()
	// TODO(tsileo) send the size along the hashes ?
	defer close(blobs)
	enum, _, err := backend.index.db.Seek([]byte(""))
	if err != nil {
		return err
	}
	for {
		k, _, err := enum.Next()
		if err == io.EOF {
			break
		}
		blobs <- string(k)
	}
	return nil
}
