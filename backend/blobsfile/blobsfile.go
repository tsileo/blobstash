/*

Package blobsfile implement the BlobsFile backend for storing blobs.

It stores multiple blobs (optionally compressed with Snappy) inside "BlobsFile"/fat file/packed file
(256MB by default) (every new file are "fallocate"d).
Blobs are indexed by a kv file.

New blobs are appended to the current file, and when the file exceed the limit, a new fie is created.

Blobs are stored with its hash and its size followed by the blob itself, thus allowing re-indexing.

	Blob hash (20 bytesà + Blob size (4 byte, uint32 binary encoded) + Blob data

Blobs are indexed by a BlobPos entry (value stored as string):

	Blob Hash => n (BlobFile index) + (space) + offset + (space) + Blob size

Write-only mode

The backend also support a write-only mode (Get operation is disabled in this mode)
where older blobsfile aren't needed since they won't be loaded (used for cold storage,
once uploaded, a blobsfile can be removed but are kept in the index, and since no metadata
is needed for blobs, this format is better than a tar archive).

*/
package blobsfile

import (
	"crypto/sha1"
	"encoding/binary"
	"expvar"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"bytes"
	"syscall"

	"code.google.com/p/snappy-go/snappy"
	"github.com/bitly/go-simplejson"
)

const (
	magic = "\x00Blobs"
)

const (
	defaultMaxBlobsFileSize = 256 << 20 // 256MB
)

const (
	Overhead = 24 // 24 bytes of meta-data are stored for head bytes
)

var (
	openFdsVar      = expvar.NewMap("blobsfile-open-fds")
	bytesUploaded   = expvar.NewMap("blobsfile-bytes-uploaded")
	bytesDownloaded = expvar.NewMap("blobsfile-bytes-downloaded")
	blobsUploaded   = expvar.NewMap("blobsfile-blobs-uploaded")
	blobsDownloaded = expvar.NewMap("blobsfile-blobs-downloaded")
)

// SHA1 is a helper to quickly compute the SHA1 hash of a []byte.
func SHA1(data []byte) string {
	h := sha1.New()
	h.Write(data)
	return fmt.Sprintf("%x", h.Sum(nil))
}

type BlobsFileBackend struct {
	// Directory which holds the blobsfile
	Directory string

	// Maximum size for a blobsfile (256MB by default)
	maxBlobsFileSize int64

	// Backend state
	loaded      bool
	reindexMode bool

	// WriteOnly mode (if the precedent blobs are not available yet)
	writeOnly bool

	// Compression is disabled by default
	snappyCompression bool

	index *BlobsIndex

	// Current blobs file opened for write
	n       int
	current *os.File
	// Size of the current blobs file
	size int64
	// All blobs files opened for read
	files map[int]*os.File

	sync.Mutex
}

func New(dir string, maxBlobsFileSize int64, compression, writeOnly bool) *BlobsFileBackend {
	log.Println("BlobsFileBackend: starting, opening index")
	os.Mkdir(dir, 0744)
	var reindex bool
	if _, err := os.Stat(filepath.Join(dir, "blobs-index")); os.IsNotExist(err) {
		reindex = true
	}
	index, err := NewIndex(dir)
	if err != nil {
		panic(err)
	}
	if maxBlobsFileSize == 0 {
		maxBlobsFileSize = defaultMaxBlobsFileSize
	}
	backend := &BlobsFileBackend{Directory: dir, snappyCompression: compression,
		index: index, files: make(map[int]*os.File), writeOnly: writeOnly,
		maxBlobsFileSize: maxBlobsFileSize, reindexMode: reindex}

	loader := backend.load
	if backend.writeOnly {
		loader = backend.loadWriteOnly
	}
	if err := loader(); err != nil {
		panic(fmt.Errorf("Error loading %T: %v", backend, err))
	}
	log.Printf("BlobsFileBackend: snappyCompression = %v", backend.snappyCompression)
	log.Printf("BlobsFileBackend: backend id => %v", backend.String())
	return backend
}

// NewFromConfig initialize a BlobsFileBackend from a JSON object.
func NewFromConfig(conf *simplejson.Json) *BlobsFileBackend {
	return New(conf.Get("path").MustString("./backend_blobsfile"),
		conf.Get("blobsfile-max-size").MustInt64(0),
		conf.Get("compression").MustBool(false),
		conf.Get("write-only").MustBool(false))
}

// Len compute the number of blobs stored
func (backend *BlobsFileBackend) Len() int {
	storedBlobs := make(chan string)
	go backend.Enumerate(storedBlobs)
	cnt := 0
	for _ = range storedBlobs {
		cnt++
	}
	return cnt
}

func (backend *BlobsFileBackend) Close() {
	log.Println("BlobsFileBackend: closing index")
	backend.index.Close()
}

func (backend *BlobsFileBackend) Done() error {
	if backend.writeOnly {
		// Switch file and delete older file
		return nil
	}
	return nil
}

func (backend *BlobsFileBackend) Remove() {
	backend.index.Remove()
}

func (backend *BlobsFileBackend) saveN() error {
	return backend.index.SetN(backend.n)
}

func (backend *BlobsFileBackend) restoreN() error {
	n, err := backend.index.GetN()
	if err != nil {
		return err
	}
	backend.n = n
	return nil
}

func (backend *BlobsFileBackend) String() string {
	if backend.writeOnly {
		return fmt.Sprintf("blobsfile-write-only-%v", backend.Directory)
	}
	return fmt.Sprintf("blobsfile-%v", backend.Directory)
}

// reindex scans all BlobsFile and reconstruct the index from scratch.
func (backend *BlobsFileBackend) reindex() error {
	log.Printf("BlobsFileBackend: re-indexing BlobsFiles...")
	if backend.writeOnly {
		panic("can't re-index in write-only mode")
	}
	if backend.Len() != 0 {
		panic("can't re-index, an non-empty backend already exists")
	}
	n := 0
	for {
		err := backend.ropen(n)
		if os.IsNotExist(err) {
			break
		}
		if err != nil {
			return err
		}
		offset := 6
		blobsfile := backend.files[n]
		blobsIndexed := 0
		for {
			// SCAN
			blobHash := make([]byte, sha1.Size)
			blobSizeEncoded := make([]byte, 4)
			_, err := blobsfile.Read(blobHash)
			if err == io.EOF {
				break
			}
			_, err = blobsfile.Read(blobSizeEncoded)
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			blobSize := binary.LittleEndian.Uint32(blobSizeEncoded)
			rawBlob := make([]byte, int(blobSize))
			read, err := blobsfile.Read(rawBlob)
			if err != nil || read != int(blobSize) {
				return fmt.Errorf("error while reading raw blob: %v", err)
			}
			blobPos := BlobPos{n: n, offset: offset, size: int(blobSize)}
			offset += Overhead+int(blobSize)
			var blob []byte
			if backend.snappyCompression {
				blobDecoded, err := snappy.Decode(nil, rawBlob)
				if err != nil {
					return fmt.Errorf("failed to decode blob")
				}
				blob = blobDecoded
			} else {
				blob = rawBlob
			}
			hash := SHA1(blob)
			if fmt.Sprintf("%x", blobHash) != hash {
				return fmt.Errorf("hash doesn't match %v/%v", fmt.Sprintf("%x", blobHash), hash)
			}
			if err := backend.index.SetPos(hash, blobPos); err != nil {
				return err
			}
			blobsIndexed++
		}
		log.Printf("BlobsFileBackend: %v re-indexed (%v blobs)", backend.filename(n), blobsIndexed)
		n++
	}
	if n == 0 {
		log.Println("BlobsFileBackend: no BlobsFiles found for re-indexing")
		return nil
	}
	if err := backend.saveN(); err != nil {
		return err
	}
	return nil
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
		if err := backend.saveN(); err != nil {
			return err
		}
		backend.loaded = true
		return nil
	}
	// Open the last file for write
	if err := backend.wopen(n - 1); err != nil {
		return err
	}
	if err := backend.saveN(); err != nil {
		return err
	}
	backend.loaded = true
	if backend.reindexMode {
		if err := backend.reindex(); err != nil {
			return err
		}
	}
	return nil
}

// When the backend is in write-only mode, this function is called instead of load().
func (backend *BlobsFileBackend) loadWriteOnly() error {
	log.Println("BlobsFileBackend: write-only mode enabled")
	if err := backend.restoreN(); err != nil {
		return err
	}
	backend.n++
	if err := backend.wopen(backend.n); err != nil {
		return err
	}
	if err := backend.saveN(); err != nil {
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
			openFdsVar.Add(backend.Directory, -1)
			return err
		}
	}
	created := false
	if _, err := os.Stat(backend.filename(n)); os.IsNotExist(err) {
		created = true
	}
	f, err := os.OpenFile(backend.filename(n), os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	backend.current = f
	backend.n = n
	if created {
		if ferr := backend.allocateBlobsFile(); ferr != nil {
			log.Printf("BlobsFileBackend: fallocate file %v error: %v", backend.filename(n), ferr)
		}
		// Write the header/magic number
		_, err := backend.current.Write([]byte(magic))
		if err != nil {
			return err
		}
		if err = backend.current.Sync(); err != nil {
			panic(err)
		}
	}
	backend.size, err = f.Seek(0, os.SEEK_END)
	if err != nil {
		return err
	}
	openFdsVar.Add(backend.Directory, 1)
	return nil
}

// Open a file for read
func (backend *BlobsFileBackend) ropen(n int) error {
	if backend.writeOnly {
		panic("backend is in write-only mode")
	}
	_, alreadyOpen := backend.files[n]
	if alreadyOpen {
		log.Printf("BlobsFileBackend: blobsfile %v already open", backend.filename(n))
		return nil
	}
	if n > len(backend.files) {
		return fmt.Errorf("Trying to open file %v whereas only %v files currently open", n, len(backend.files))
	}

	filename := backend.filename(n)
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	fmagic := make([]byte, len(magic))
	_, err = f.Read(fmagic)
	if err != nil || magic != string(fmagic) {
		return fmt.Errorf("magic not found in BlobsFile")
	}
	backend.files[n] = f
	openFdsVar.Add(backend.Directory, 1)
	return nil
}

// Generate a new blobs file and fallocate a 256MB file.
func (backend *BlobsFileBackend) allocateBlobsFile() error {
	log.Printf("BlobsFileBackend: running fallocate on BlobsFile %v", backend.filename(backend.n))
	// fallocate 256MB
	if err := syscall.Fallocate(int(backend.current.Fd()), 0x01, 0, backend.maxBlobsFileSize); err != nil {
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
	blobSize, blobEncoded := backend.encodeBlob(data)
	blobPos := BlobPos{n: backend.n, offset: int(backend.size), size: blobSize}
	if err := backend.index.SetPos(hash, blobPos); err != nil {
		return err
	}
	n, err := backend.current.Write(blobEncoded)
	backend.size += int64(len(blobEncoded))
	if err != nil || n != len(blobEncoded) {
		return fmt.Errorf("Error writing blob (%v,%v)", err, n)
	}
	if err = backend.current.Sync(); err != nil {
		panic(err)
	}
	bytesUploaded.Add(backend.Directory, int64(len(blobEncoded)))
	blobsUploaded.Add(backend.Directory, 1)

	if backend.size > backend.maxBlobsFileSize {
		backend.n++
		log.Printf("BlobsFileBackend: creating a new BlobsFile")
		if err := backend.wopen(backend.n); err != nil {
			panic(err)
		}
		if !backend.writeOnly {
			if err := backend.ropen(backend.n); err != nil {
				panic(err)
			}
		}
		if err := backend.saveN(); err != nil {
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

func (backend *BlobsFileBackend) decodeBlob(data []byte) (size int, blob []byte) {
	size = int(binary.LittleEndian.Uint32(data[sha1.Size:Overhead]))
	blob = make([]byte, size)
	copy(blob, data[Overhead:])
	if backend.snappyCompression {
		blobDecoded, err := snappy.Decode(nil, blob)
		if err != nil {
			panic(fmt.Errorf("Failed to decode blob with Snappy: %v", err))
		}
		blob = blobDecoded
	}
	h := sha1.New()
	h.Write(blob)
	if !bytes.Equal(h.Sum(nil), data[0:sha1.Size]) {
		panic(fmt.Errorf("Hash doesn't match %x != %x", h.Sum(nil), data[0:sha1.Size]))
	}
	return
}

func (backend *BlobsFileBackend) encodeBlob(blob []byte) (size int, data []byte) {
	h := sha1.New()
	h.Write(blob)

	if backend.snappyCompression {
		dataEncoded, err := snappy.Encode(nil, blob)
		if err != nil {
			panic(fmt.Errorf("Failed to encode blob with Snappy: %v", err))
		}
		blob = dataEncoded
	}
	size = len(blob)
	data = make([]byte, len(blob)+Overhead)
	copy(data[:], h.Sum(nil))
	binary.LittleEndian.PutUint32(data[sha1.Size:], uint32(size))
	copy(data[Overhead:], blob)
	return
}

func (backend *BlobsFileBackend) Get(hash string) ([]byte, error) {
	if !backend.loaded {
		panic("backend BlobsFileBackend not loaded")
	}
	if backend.writeOnly {
		panic("backend is in write-only mode")
	}
	blobPos, err := backend.index.GetPos(hash)
	if err != nil {
		return nil, fmt.Errorf("Error fetching GetPos: %v", err)
	}
	if blobPos == nil {
		return nil, fmt.Errorf("Blob %v not found in index", err)
	}
	data := make([]byte, blobPos.size+Overhead)
	n, err := backend.files[blobPos.n].ReadAt(data, int64(blobPos.offset))
	if err != nil {
		return nil, fmt.Errorf("Error reading blob: %v / blobsfile: %+v", err, backend.files[blobPos.n])
	}
	if n != blobPos.size+Overhead {
		return nil, fmt.Errorf("Error reading blob %v, read %v, expected %v+%v", hash, n, blobPos.size, Overhead)
	}
	blobSize, blob := backend.decodeBlob(data)
	if blobSize != blobPos.size {
		return nil, fmt.Errorf("Bad blob %v encoded size, got %v, expected %v", hash, n, blobSize)
	}
	bytesDownloaded.Add(backend.Directory, int64(blobSize))
	blobsUploaded.Add(backend.Directory, 1)
	return blob, nil
}

func (backend *BlobsFileBackend) Enumerate(blobs chan<- string) error {
	if !backend.loaded {
		panic("backend BlobsFileBackend not loaded")
	}
	backend.Lock()
	defer backend.Unlock()
	// TODO(tsileo) send the size along the hashes ?
	defer close(blobs)
	enum, _, err := backend.index.db.Seek(formatKey(BlobPosKey, ""))
	if err != nil {
		return err
	}
	for {
		k, _, err := enum.Next()
		if err == io.EOF {
			break
		}
		// Remove the BlobPosKey prefix byte
		blobs <- string(k[1:])
	}
	return nil
}
