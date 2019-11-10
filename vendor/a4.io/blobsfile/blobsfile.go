/*

Package blobsfile implement the BlobsFile backend for storing blobs.

It stores multiple blobs (optionally compressed with Snappy) inside "BlobsFile"/fat file/packed file
(256MB by default).
Blobs are indexed by a kv file (that can be rebuild from the blobsfile).

New blobs are appended to the current file, and when the file exceed the limit, a new fie is created.

*/
package blobsfile // import "a4.io/blobsfile"

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"expvar"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"a4.io/blobstash/pkg/rangedb"
	"github.com/golang/snappy"
	"github.com/klauspost/reedsolomon"
	"golang.org/x/crypto/blake2b"
)

const (
	// Version is the current BlobsFile binary format version
	Version = 1

	headerMagic = "\x00Blobs"
	headerSize  = len(headerMagic) + 58 // magic + 58 reserved bytes

	// 38 bytes of meta-data are stored for each blob: 32 byte hash + 2 byte flag + 4 byte blob len
	blobOverhead = 38
	hashSize     = 32

	// Reed-Solomon config
	dataShards   = 10 // 10 data shards
	parityShards = 2  // 2 parity shards

	defaultMaxBlobsFileSize = 256 << 20 // 256MB
)

// Blob flags
const (
	flagBlob byte = 1 << iota
	flagCompressed
	flagParityBlob
	flagEOF
)

type CompressionAlgorithm byte

// Compression algorithms flag
const (
	Snappy CompressionAlgorithm = 1 << iota
)

var (
	openFdsVar      = expvar.NewMap("blobsfile-open-fds")
	bytesUploaded   = expvar.NewMap("blobsfile-bytes-uploaded")
	bytesDownloaded = expvar.NewMap("blobsfile-bytes-downloaded")
	blobsUploaded   = expvar.NewMap("blobsfile-blobs-uploaded")
	blobsDownloaded = expvar.NewMap("blobsfile-blobs-downloaded")
)

var (
	// ErrBlobNotFound reports that the blob could not be found
	ErrBlobNotFound = errors.New("blob not found")

	// ErrBlobsfileCorrupted reports that one of the BlobsFile is corrupted and could not be repaired
	ErrBlobsfileCorrupted = errors.New("blobsfile is corrupted")

	errParityBlobCorrupted = errors.New("a parity blob is corrupted")
)

// ErrInterventionNeeded is an error indicating an manual action must be performed before being able to use BobsFile
type ErrInterventionNeeded struct {
	msg string
}

func (ein *ErrInterventionNeeded) Error() string {
	return fmt.Sprintf("manual intervention needed: %s", ein.msg)
}

func checkFlag(f byte) {
	if f == flagEOF || f == flagParityBlob {
		panic(fmt.Sprintf("Unexpected blob flag %v", f))
	}
}

// multiError wraps multiple errors in a single one
type multiError struct {
	errors []error
}

func (me *multiError) Error() string {
	if me.errors == nil {
		return "multiError:"
	}
	var errs []string
	for _, err := range me.errors {
		errs = append(errs, err.Error())
	}
	return fmt.Sprintf("multiError: %s", strings.Join(errs, ", "))
}

func (me *multiError) Append(err error) {
	me.errors = append(me.errors, err)
}

func (me *multiError) Nil() bool {
	if me.errors == nil || len(me.errors) == 0 {
		return true
	}
	return false
}

// corruptedError give more about the corruption of a BlobsFile
type corruptedError struct {
	n      int
	blobs  []*blobPos
	offset int64
	err    error
}

func (ce *corruptedError) Error() string {
	if len(ce.blobs) > 0 {
		return fmt.Sprintf("%d blobs are corrupt", len(ce.blobs))
	}
	return fmt.Sprintf("corrupted at offset %d: %v", ce.offset, ce.err)
}

func (ce *corruptedError) firstBadOffset() int64 {
	if len(ce.blobs) > 0 {
		off := int64(ce.blobs[0].offset)
		if ce.offset == -1 || off < ce.offset {
			return off
		}
	}
	return ce.offset
}

func firstCorruptedShard(offset int64, shardSize int) int {
	i := 0
	ioffset := int(offset)
	for j := 0; j < dataShards; j++ {
		if shardSize+(shardSize*i) > ioffset {
			return i
		}
		i++
	}
	return 0
}

// Stats represents some stats about the DB state
type Stats struct {
	// The total number of blobs stored
	BlobsCount int

	// The size of all the blobs stored
	BlobsSize int64

	// The number of BlobsFile
	BlobsFilesCount int

	// The size of all the BlobsFile
	BlobsFilesSize int64
}

// Opts represents the DB options
type Opts struct {
	// Compression algorithm
	Compression CompressionAlgorithm

	// The max size of a BlobsFile, will be 256MB by default if not set
	BlobsFileSize int64

	// Where the data and indexes will be stored
	Directory string

	// Allow to catch some events
	LogFunc func(msg string)

	// When trying to self-heal in case of recovery, some step need to be performed by the user
	AskConfirmationFunc func(msg string) bool

	BlobsFilesSealedFunc func(path string)

	// Not implemented yet, will allow to provide repaired data in case of hard failure
	// RepairBlobFunc func(hash string) ([]byte, error)
}

func (o *Opts) init() {
	if o.BlobsFileSize == 0 {
		o.BlobsFileSize = defaultMaxBlobsFileSize
	}
}

// BlobsFiles represent the DB
type BlobsFiles struct {
	// Directory which holds the blobsfile
	directory string

	// Maximum size for a blobsfile (256MB by default)
	maxBlobsFileSize int64

	// Backend state
	reindexMode bool

	// Compression is disabled by default
	compression CompressionAlgorithm

	// The kv index that maintains blob positions
	index *blobsIndex

	// Current blobs file opened for write
	n       int
	current *os.File
	// Size of the current blobs file
	size int64
	// All blobs files opened for read
	files map[int]*os.File

	lastErr      error
	lastErrMutex sync.Mutex // mutex for guarding the lastErr

	logFunc              func(string)
	askConfirmationFunc  func(string) bool
	blobsFilesSealedFunc func(string)

	// Reed-solomon encoder for the parity blobs
	rse reedsolomon.Encoder

	wg sync.WaitGroup
	sync.Mutex
}

// Blob represents a blob hash and size when enumerating the DB.
type Blob struct {
	Hash string
	Size int
	N    int
}

// New intializes a new BlobsFileBackend.
func New(opts *Opts) (*BlobsFiles, error) {
	opts.init()
	dir := opts.Directory
	// Try to create the directory
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	var reindex bool
	// Check if an index file is already present
	if _, err := os.Stat(filepath.Join(dir, "blobs-index")); os.IsNotExist(err) {
		// No index found
		reindex = true
	}
	index, err := newIndex(dir)
	if err != nil {
		return nil, err
	}

	// Initialize the Reed-Solomon encoder
	enc, err := reedsolomon.New(dataShards, parityShards)
	if err != nil {
		return nil, err
	}
	backend := &BlobsFiles{
		directory:            dir,
		compression:          opts.Compression,
		index:                index,
		files:                make(map[int]*os.File),
		maxBlobsFileSize:     opts.BlobsFileSize,
		blobsFilesSealedFunc: opts.BlobsFilesSealedFunc,
		rse:                  enc,
		reindexMode:          reindex,
		logFunc:              opts.LogFunc,
	}
	if err := backend.load(); err != nil {
		panic(fmt.Errorf("error loading %T: %v", backend, err))
	}
	return backend, nil
}

func (backend *BlobsFiles) getConfirmation(msg string) (bool, error) {
	// askConfirmationFunc func(string) bool
	if backend.askConfirmationFunc == nil {
		return false, &ErrInterventionNeeded{msg}
	}

	ok := backend.askConfirmationFunc(msg)

	if !ok {
		return false, &ErrInterventionNeeded{msg}
	}

	return true, nil
}

func (backend *BlobsFiles) SealedPacks() []string {
	packs := []string{}
	for i := 0; i < backend.n; i++ {
		packs = append(packs, backend.filename(i))
	}
	return packs
}

func (backend *BlobsFiles) iterOpenFiles() (files []*os.File) {
	for _, f := range backend.files {
		files = append(files, f)
	}
	return files
}

func (backend *BlobsFiles) closeOpenFiles() {
	for _, f := range backend.files {
		f.Close()
	}
}

func (backend *BlobsFiles) log(msg string, args ...interface{}) {
	if backend.logFunc == nil {
		return
	}
	backend.logFunc(fmt.Sprintf(msg, args...))
}

// Stats returns some stats about the DB.
func (backend *BlobsFiles) Stats() (*Stats, error) {
	// Iterate the index to gather the stats (Enumerate will acquire the lock)
	bchan := make(chan *Blob)
	errc := make(chan error, 1)
	go func() {
		errc <- backend.Enumerate(bchan, "", "\xfe", 0)
	}()
	blobsCount := 0
	var blobsSize int64
	for ref := range bchan {
		blobsCount++
		blobsSize += int64(ref.Size)
	}
	if err := <-errc; err != nil {
		panic(err)
	}

	// Now iterate the raw blobsfile for gethering stats
	backend.Lock()
	defer backend.Unlock()
	var bfs int64
	for _, f := range backend.iterOpenFiles() {
		finfo, err := f.Stat()
		if err != nil {
			return nil, err
		}
		bfs += finfo.Size()
	}
	n, err := backend.getN()
	if err != nil {
		return nil, err
	}

	return &Stats{
		BlobsFilesCount: n + 1,
		BlobsFilesSize:  bfs,
		BlobsCount:      blobsCount,
		BlobsSize:       blobsSize,
	}, nil
}

// setLastError is used by goroutine that can't return an error easily
func (backend *BlobsFiles) setLastError(err error) {
	backend.lastErrMutex.Lock()
	defer backend.lastErrMutex.Unlock()
	backend.lastErr = err
}

// lastError returns the last error that may have happened in asynchronous way (like the parity blobs writing process).
func (backend *BlobsFiles) lastError() error {
	backend.lastErrMutex.Lock()
	defer backend.lastErrMutex.Unlock()
	if backend.lastErr == nil {
		return nil
	}
	err := backend.lastErr
	backend.lastErr = nil
	return err
}

// Close closes all the indexes and data files.
func (backend *BlobsFiles) Close() error {
	backend.wg.Wait()
	if err := backend.lastError(); err != nil {
		return err
	}
	if err := backend.index.Close(); err != nil {
		return err
	}
	return nil
}

// RebuildIndex removes the index files and re-build it by re-scanning all the BlobsFiles.
func (backend *BlobsFiles) RebuildIndex() error {
	if err := backend.index.remove(); err != nil {
		return nil
	}
	return backend.reindex()
}

// getN returns the total numbers of BlobsFile.
func (backend *BlobsFiles) getN() (int, error) {
	return backend.index.getN()
}

func (backend *BlobsFiles) saveN() error {
	return backend.index.setN(backend.n)
}

func (backend *BlobsFiles) restoreN() error {
	n, err := backend.index.getN()
	if err != nil {
		return err
	}
	backend.n = n
	return nil
}

// String implements the Stringer interface.
func (backend *BlobsFiles) String() string {
	return fmt.Sprintf("blobsfile-%v", backend.directory)
}

// scanBlobsFile scan a single BlobsFile (#n), and execute `iterFunc` for each indexed blob.
// `iterFunc` is optional, and without it, this func will check the consistency of each blob, and return
// a `corruptedError` if a blob is corrupted.
func (backend *BlobsFiles) scanBlobsFile(n int, iterFunc func(*blobPos, byte, string, []byte) error) error {
	corrupted := []*blobPos{}

	// Ensure this BlosFile is open
	err := backend.ropen(n)
	if err != nil {
		return err
	}

	// Seek at the start of data
	offset := int64(headerSize)
	blobsfile := backend.files[n]
	if _, err := blobsfile.Seek(int64(headerSize), os.SEEK_SET); err != nil {
		return err
	}

	blobsIndexed := 0

	blobHash := make([]byte, hashSize)
	blobSizeEncoded := make([]byte, 4)
	flags := make([]byte, 2)

	for {
		// Read the hash
		if _, err := blobsfile.Read(blobHash); err != nil {
			if err == io.EOF {
				break
			}
			return &corruptedError{n, nil, offset, fmt.Errorf("failed to read hash: %v", err)}
		}

		// Read the 2 byte flags
		if _, err := blobsfile.Read(flags); err != nil {
			return &corruptedError{n, nil, offset, fmt.Errorf("failed to read flag: %v", err)}
		}

		// If we reached the EOF blob, we're done
		if flags[0] == flagEOF {
			break
		}

		// Read the size of the blob
		if _, err := blobsfile.Read(blobSizeEncoded); err != nil {
			return &corruptedError{n, nil, offset, fmt.Errorf("failed to read blob size: %v", err)}
		}

		// Read the actual blob
		blobSize := int64(binary.LittleEndian.Uint32(blobSizeEncoded))
		rawBlob := make([]byte, int(blobSize))
		read, err := blobsfile.Read(rawBlob)
		if err != nil || read != int(blobSize) {
			return &corruptedError{n, nil, offset, fmt.Errorf("error while reading raw blob: %v", err)}
		}

		// Build the `blobPos`
		blobPos := &blobPos{n: n, offset: offset, size: int(blobSize)}
		offset += blobOverhead + blobSize

		// Decompress the blob if needed
		var blob []byte
		if flags[0] == flagCompressed && flags[1] != 0 {
			var err error
			var blobDecoded []byte
			switch CompressionAlgorithm(flags[1]) {
			case Snappy:
				blobDecoded, err = snappy.Decode(nil, rawBlob)
			}
			if err != nil {
				return &corruptedError{n, nil, offset, fmt.Errorf("failed to decode blob: %v %v %v", err, blobSize, flags)}
			}
			blob = blobDecoded

		} else {
			blob = rawBlob
		}
		// Store the real blob size (i.e. the decompressed size if the data is compressed)
		blobPos.blobSize = len(blob)

		// Ensure the blob is not corrupted
		hash := fmt.Sprintf("%x", blake2b.Sum256(blob))
		if fmt.Sprintf("%x", blobHash) == hash {
			if iterFunc != nil {
				if err := iterFunc(blobPos, flags[0], hash, blob); err != nil {
					return err
				}
			}
			blobsIndexed++
		} else {
			// The blobs is corrupted, keep track of it
			corrupted = append(corrupted, blobPos)
		}
	}

	if len(corrupted) > 0 {
		return &corruptedError{n, corrupted, -1, nil}
	}

	return nil
}

// scanBlobsFile scan a single BlobsFile (#n), and execute `iterFunc` for each indexed blob.
// `iterFunc` is optional, and without it, this func will check the consistency of each blob, and return
// a `corruptedError` if a blob is corrupted.
func ScanBlobsFile(path string) ([]string, error) {
	hashes := []string{}
	blobsfile, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer blobsfile.Close()

	// Seek at the start of data
	offset := int64(headerSize)
	if _, err := blobsfile.Seek(int64(headerSize), os.SEEK_SET); err != nil {
		return nil, err
	}

	blobsIndexed := 0

	blobHash := make([]byte, hashSize)
	blobSizeEncoded := make([]byte, 4)
	flags := make([]byte, 2)

	for {
		// Read the hash
		if _, err := blobsfile.Read(blobHash); err != nil {
			if err == io.EOF {
				break
			}
			return nil, &corruptedError{0, nil, offset, fmt.Errorf("failed to read hash: %v", err)}
		}

		// Read the 2 byte flags
		if _, err := blobsfile.Read(flags); err != nil {
			return nil, &corruptedError{0, nil, offset, fmt.Errorf("failed to read flag: %v", err)}
		}

		// If we reached the EOF blob, we're done
		if flags[0] == flagEOF {
			break
		}

		// Read the size of the blob
		if _, err := blobsfile.Read(blobSizeEncoded); err != nil {
			return nil, &corruptedError{0, nil, offset, fmt.Errorf("failed to read blob size: %v", err)}
		}

		// Read the actual blob
		blobSize := int64(binary.LittleEndian.Uint32(blobSizeEncoded))
		rawBlob := make([]byte, int(blobSize))
		read, err := blobsfile.Read(rawBlob)
		if err != nil || read != int(blobSize) {
			return nil, &corruptedError{0, nil, offset, fmt.Errorf("error while reading raw blob: %v", err)}
		}

		// Build the `blobPos`
		offset += blobOverhead + blobSize

		// Decompress the blob if needed
		var blob []byte
		if flags[0] == flagCompressed && flags[1] != 0 {
			var err error
			var blobDecoded []byte
			switch CompressionAlgorithm(flags[1]) {
			case Snappy:
				blobDecoded, err = snappy.Decode(nil, rawBlob)
			}
			if err != nil {
				return nil, &corruptedError{0, nil, offset, fmt.Errorf("failed to decode blob: %v %v %v", err, blobSize, flags)}
			}
			blob = blobDecoded

		} else {
			blob = rawBlob
		}

		// Ensure the blob is not corrupted
		hash := fmt.Sprintf("%x", blake2b.Sum256(blob))
		if fmt.Sprintf("%x", blobHash) == hash {
			hashes = append(hashes, hash)
			blobsIndexed++
		} else {
			panic("corrupted")
		}
	}

	return hashes, nil
}

func copyShards(i [][]byte) (o [][]byte) {
	for _, a := range i {
		o = append(o, a)
	}
	return o
}

// CheckBlobsFiles will check the consistency of all the BlobsFile
func (backend *BlobsFiles) CheckBlobsFiles() error {
	err := backend.scan(nil)
	if err == nil {
		backend.log("all blobs has been verified")
	}
	return err
}

func (backend *BlobsFiles) checkBlobsFile(cerr *corruptedError) error {
	// TODO(tsileo): provide an exported method to do the check
	n := cerr.n
	pShards, err := backend.parityShards(n)
	if err != nil {
		// TODO(tsileo): log the error
		fmt.Printf("parity shards err=%v\n", err)
	}
	parityCnt := len(pShards)
	fmt.Printf("scan result=%v %+v\n", cerr, cerr)
	// if err == nil && (pShards == nil || len(pShards) != parityShards) {
	// 	// We can rebuild the parity blobs if needed
	// 	// FIXME(tsileo): do it
	// 	var l int
	// 	if pShards != nil {
	// 		l = len(pShards)
	// 	} else {
	// 		pShards = [][]byte{}
	// 	}

	// 	for i := 0; i < parityShards-l; i++ {
	// 		pShards = append(pShards, nil)
	// 	}
	// 	// TODO(tsileo): save the parity shards
	// }

	if pShards == nil || len(pShards) == 0 {
		return fmt.Errorf("no parity shards available, can't recover")
	}

	dataShardIndex := 0
	if cerr != nil {
		badOffset := cerr.firstBadOffset()
		fmt.Printf("badOffset: %v\n", badOffset)
		dataShardIndex = firstCorruptedShard(badOffset, int(backend.maxBlobsFileSize)/dataShards)
		fmt.Printf("dataShardIndex=%d\n", dataShardIndex)
	}

	// if err != nil {
	// 	if cerr, ok := err.(*corruptedError); ok {
	// 		badOffset := cerr.firstBadOffset()
	// 		fmt.Printf("badOffset: %v\n", badOffset)
	// 		dataShardIndex = firstCorruptedShard(badOffset, int(backend.maxBlobsFileSize)/dataShards)
	// 		fmt.Printf("dataShardIndex=%d\n", dataShardIndex)
	// 	}
	// }

	missing := []int{}
	for i := dataShardIndex; i < 10; i++ {
		missing = append(missing, i)
	}
	fmt.Printf("missing=%+v\n", missing)

	dShards, err := backend.dataShards(n)
	if err != nil {
		return err
	}

	fmt.Printf("try #1\n")
	if len(missing) <= parityCnt {
		shards := copyShards(append(dShards, pShards...))

		for _, idx := range missing {
			shards[idx] = nil
		}

		if err := backend.rse.Reconstruct(shards); err != nil {
			return err
		}

		ok, err := backend.rse.Verify(shards)
		if err != nil {
			return err
		}

		if ok {
			fmt.Printf("reconstruct successful\n")
			if err := backend.rewriteBlobsFile(n, shards); err != nil {
				return err
			}

			return nil
		}
		return fmt.Errorf("unrecoverable corruption")
	}

	fmt.Printf("try #2\n")
	// Try one missing shards
	for i := dataShardIndex; i < 10; i++ {
		shards := copyShards(append(dShards, pShards...))
		shards[i] = nil

		if err := backend.rse.Reconstruct(shards); err != nil {
			return err
		}

		ok, err := backend.rse.Verify(shards)
		if err != nil {
			return err
		}

		if ok {
			fmt.Printf("reconstruct successful at %d\n", i)
			if err := backend.rewriteBlobsFile(n, shards); err != nil {
				return err
			}

			return nil
		}
	}

	// TODO(tsileo): only do this check if the two parity blobs are here
	fmt.Printf("try #3\n")
	if len(pShards) >= 2 {
		for i := dataShardIndex; i < 10; i++ {
			for j := dataShardIndex; j < 10; j++ {
				if j == i {
					continue
				}

				shards := copyShards(append(dShards, pShards...))

				shards[i] = nil
				shards[j] = nil

				if err := backend.rse.Reconstruct(shards); err != nil {
					return err
				}

				ok, err := backend.rse.Verify(shards)
				if err != nil {
					return err
				}

				if ok {
					if err := backend.rewriteBlobsFile(n, shards); err != nil {
						return err
					}

					return nil
				}
			}
		}
	}

	// XXX(tsileo): support for 4 failed parity shards
	return fmt.Errorf("failed to recover")
}

func (backend *BlobsFiles) rewriteBlobsFile(n int, shards [][]byte) error {
	if f, alreadyOpen := backend.files[n]; alreadyOpen {
		if err := f.Close(); err != nil {
			return err
		}
		delete(backend.files, n)
	}

	// Create a new temporary file
	f, err := os.OpenFile(backend.filename(n)+".new", os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}

	// Re-create the healed Blobsfile
	for _, shard := range shards[0:dataShards] {
		f.Write(shard)
	}
	for _, shard := range shards[dataShards:] {
		_, parityBlobEncoded := backend.encodeBlob(shard, flagParityBlob)

		n, err := f.Write(parityBlobEncoded)
		if err != nil || n != len(parityBlobEncoded) {
			return fmt.Errorf("error writing parity blob (%v,%v)", err, n)
		}
	}

	if err := f.Sync(); err != nil {
		return err
	}
	f.Close()

	// Remove the corrupted BlobsFile
	if err := os.Remove(backend.filename(n)); err != nil {
		return err
	}

	// Rename our newly created BlobsFile to replace the old one
	if err := os.Rename(backend.filename(n)+".new", backend.filename(n)); err != nil {
		return err
	}

	fmt.Printf("reopen\n")
	if err := backend.ropen(n); err != nil {
		return err
	}
	fmt.Printf("file rewrite done\n")
	// if err := f.Close(); err != nil {
	// 	return err
	// }

	// TODO(tsileo): display user info (introduce a new helper) to ask to remove the old blobsfile and rename the
	// .restored.
	// TODO(tsileo): also use this new helper (which should clean shutdown blobstahs) in case of blbo corruption
	// detected.
	// TODO(tsileo): also prove a call for corruptions to let wrapper provide a repaired blob from other source.
	return nil
}

func (backend *BlobsFiles) dataShards(n int) ([][]byte, error) {
	// Read the whole blobsfile data (except the parity blobs)
	data := make([]byte, backend.maxBlobsFileSize)
	if _, err := backend.files[n].ReadAt(data, 0); err != nil {
		return nil, err
	}

	if !bytes.Equal(data[0:len(headerMagic)], []byte(headerMagic)) {
		return nil, fmt.Errorf("bad magic when trying to creata data shard")
	}
	fmt.Printf("data shard magic OK\n")

	// Rebuild the data shards using the data part of the blobsfile
	shards, err := backend.rse.Split(data)
	if err != nil {
		return nil, err
	}

	return shards[:10], nil
}

// parityShards extract the "parity blob" at the end of the BlobsFile
func (backend *BlobsFiles) parityShards(n int) ([][]byte, error) {
	blobsfile := backend.files[n]
	parityBlobs := [][]byte{}

	merr := &multiError{}

	blobHash := make([]byte, hashSize)
	for i := 0; i < parityShards; i++ {
		// Seek to the offset where the parity blob should be stored
		offset := backend.maxBlobsFileSize + int64(i)*((backend.maxBlobsFileSize/int64(dataShards))+int64(hashSize+6))
		if _, err := backend.files[n].Seek(offset, os.SEEK_SET); err != nil {
			merr.Append(fmt.Errorf("failed to seek to parity shards: %v", err))
			parityBlobs = append(parityBlobs, nil)
			continue
		}

		// Read the hash of the blob
		if _, err := blobsfile.Read(blobHash); err != nil {
			if err == io.EOF {
				merr.Append(fmt.Errorf("missing parity blob %d, only found %d", i, len(parityBlobs)+1))
				parityBlobs = append(parityBlobs, nil)
				continue
			}
			merr.Append(fmt.Errorf("failed to read the hash for parity blob %d: %v", i, err))
			parityBlobs = append(parityBlobs, nil)
			continue
		}

		// We skip the flags and the blob length as it may be corrupted and we know the length.
		if _, err := blobsfile.Seek(offset+6+hashSize, os.SEEK_SET); err != nil {
			merr.Append(fmt.Errorf("failed to seek to parity blob %d: %v", i, err))
			parityBlobs = append(parityBlobs, nil)
			continue
		}

		// Read the blob data
		blobSize := int(backend.maxBlobsFileSize / dataShards)
		blob := make([]byte, blobSize)
		read, err := blobsfile.Read(blob)
		if err != nil || read != int(blobSize) {
			merr.Append(fmt.Errorf("error while reading raw blob %d: %v", i, err))
			parityBlobs = append(parityBlobs, nil)
			continue
		}

		// Check the data against the stored hash
		hash := fmt.Sprintf("%x", blake2b.Sum256(blob))
		if fmt.Sprintf("%x", blobHash) != hash {
			merr.Append(errParityBlobCorrupted)
			parityBlobs = append(parityBlobs, nil)
			continue
		}

		parityBlobs = append(parityBlobs, blob)
	}

	if merr.Nil() {
		return parityBlobs, nil
	}

	return parityBlobs, merr
}

// checkParityBlobs ensures that the parity blobs and the the data shards can be verified (i.e integrity verification)
func (backend *BlobsFiles) checkParityBlobs(n int) error {
	dataShards, err := backend.dataShards(n)
	if err != nil {
		return fmt.Errorf("failed to build data shards: %v", err)
	}

	parityShards, err := backend.parityShards(n)
	if err != nil {
		// We just log the error
		fmt.Printf("failed to build parity shards: %v", err)
	}

	shards := append(dataShards, parityShards...)

	// Verify the integrity of the data
	ok, err := backend.rse.Verify(shards)
	if err != nil {
		return fmt.Errorf("failed to verify shards: %v", err)
	}

	if !ok {
		return ErrBlobsfileCorrupted
	}

	return nil
}

// scan executes the callback func `iterFunc` for each indexed blobs in all the available BlobsFiles.
func (backend *BlobsFiles) scan(iterFunc func(*blobPos, byte, string, []byte) error) error {
	n := 0
	for {
		err := backend.scanBlobsFile(n, iterFunc)
		if os.IsNotExist(err) {
			break
		}
		if err != nil {
			return err
		}
		n++
	}
	if n == 0 {
		return nil
	}
	return nil
}

// reindex scans all BlobsFile and reconstruct the index from scratch.
func (backend *BlobsFiles) reindex() error {
	backend.wg.Add(1)
	defer backend.wg.Done()

	var err error
	backend.index.db, err = rangedb.New(backend.index.path)
	if err != nil {
		return err
	}

	n := 0
	blobsIndexed := 0

	iterFunc := func(blobPos *blobPos, flag byte, hash string, _ []byte) error {
		// Skip parity blobs
		if flag == flagParityBlob {
			return nil
		}
		if err := backend.index.setPos(hash, blobPos); err != nil {
			return err
		}
		n = blobPos.n
		blobsIndexed++
		return nil
	}

	if err := backend.scan(iterFunc); err != nil {
		if cerr, ok := err.(*corruptedError); ok {
			if err := backend.checkBlobsFile(cerr); err != nil {
				return err
			}

			// If err was nil, then the recontruct was successful, we can try to reindex
			if err := backend.RebuildIndex(); err != nil {
				return err
			}
			return nil
		}
		return err
	}

	if n == 0 {
		return nil
	}
	if err := backend.saveN(); err != nil {
		return err
	}
	return nil
}

// Open all the blobs-XXXXX (read-only) and open the last for write
func (backend *BlobsFiles) load() error {
	backend.wg.Add(1)
	defer backend.wg.Done()

	n := 0
	for {
		err := backend.ropen(n)
		if os.IsNotExist(err) {
			// No more blobsfile
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
		if err := backend.saveN(); err != nil {
			return err
		}
		return nil
	}

	// Open the last file for write
	if err := backend.wopen(n - 1); err != nil {
		return err
	}

	if err := backend.saveN(); err != nil {
		return err
	}

	if backend.reindexMode {
		if err := backend.reindex(); err != nil {
			return err
		}
	}
	return nil
}

// Open a file for writing, will close the previously open file if any.
func (backend *BlobsFiles) wopen(n int) error {
	// Close the already opened file if any
	if backend.current != nil {
		if err := backend.current.Close(); err != nil {
			openFdsVar.Add(backend.directory, -1)
			return err
		}
	}

	// Track if we created the file
	created := false
	if _, err := os.Stat(backend.filename(n)); os.IsNotExist(err) {
		created = true
	}

	// Open the file in rw mode
	f, err := os.OpenFile(backend.filename(n), os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}

	backend.current = f
	backend.n = n

	if created {
		// Write the header/magic number
		if _, err := backend.current.Write([]byte(headerMagic)); err != nil {
			return err
		}
		// Write the reserved bytes
		reserved := make([]byte, 58)
		binary.LittleEndian.PutUint32(reserved, uint32(Version))
		if _, err := backend.current.Write(reserved[:]); err != nil {
			return err
		}

		// Fsync
		if err = backend.current.Sync(); err != nil {
			panic(err)
		}
	}

	backend.size, err = f.Seek(0, os.SEEK_END)
	if err != nil {
		return err
	}

	openFdsVar.Add(backend.directory, 1)

	return nil
}

// Open a file for read
func (backend *BlobsFiles) ropen(n int) error {
	_, alreadyOpen := backend.files[n]
	if alreadyOpen {
		// log.Printf("BlobsFileBackend: blobsfile %v already open", backend.filename(n))
		return nil
	}
	if n > len(backend.files) {
		return fmt.Errorf("trying to open file %v whereas only %v files currently open", n, len(backend.files))
	}

	filename := backend.filename(n)
	f, err := os.Open(filename)
	if err != nil {
		return err
	}

	// Ensure the header's magic is present
	fmagic := make([]byte, len(headerMagic))
	_, err = f.Read(fmagic)
	if err != nil || headerMagic != string(fmagic) {
		return fmt.Errorf("magic not found in BlobsFile: %v or header not matching", err)
	}

	if _, err := f.Seek(int64(headerSize), os.SEEK_SET); err != nil {
		return err
	}

	backend.files[n] = f
	openFdsVar.Add(backend.directory, 1)

	return nil
}

func (backend *BlobsFiles) filename(n int) string {
	return filepath.Join(backend.directory, fmt.Sprintf("blobs-%05d", n))
}

// writeParityBlobs computes and writes the 4 parity shards using Reed-Solomon 10,4 and write them at
// end the blobsfile, and write the "data size" (blobsfile size before writing the parity shards).
func (backend *BlobsFiles) writeParityBlobs(f *os.File, size int) error {
	start := time.Now()

	// this will run in a goroutine, add the task in the wait group
	backend.wg.Add(1)
	defer backend.wg.Done()

	// First we write the padding blob
	paddingLen := backend.maxBlobsFileSize - (int64(size) + blobOverhead)
	headerEOF := makeHeaderEOF(paddingLen)
	n, err := f.Write(headerEOF)
	if err != nil {
		return fmt.Errorf("failed to write EOF header: %v", err)
	}
	size += n

	padding := make([]byte, paddingLen)
	n, err = f.Write(padding)
	if err != nil {
		return fmt.Errorf("failed to write padding 0: %v", err)
	}
	size += n

	// We write the data size at the end of the file
	if _, err := f.Seek(0, os.SEEK_END); err != nil {
		return err
	}

	// Read the whole blobsfile
	fdata := make([]byte, size)
	if _, err := f.ReadAt(fdata, 0); err != nil {
		return err
	}

	// Split into shards
	shards, err := backend.rse.Split(fdata)
	if err != nil {
		return err
	}
	// Create the parity shards
	if err := backend.rse.Encode(shards); err != nil {
		return err
	}

	// Save the parity blobs
	parityBlobs := shards[dataShards:]
	for _, parityBlob := range parityBlobs {
		_, parityBlobEncoded := backend.encodeBlob(parityBlob, flagParityBlob)

		n, err := f.Write(parityBlobEncoded)
		// backend.size += int64(len(parityBlobEncoded))
		if err != nil || n != len(parityBlobEncoded) {
			return fmt.Errorf("error writing parity blob (%v,%v)", err, n)
		}
	}

	// Fsync
	if err = f.Sync(); err != nil {
		return err
	}

	if err := f.Close(); err != nil {
		return err
	}

	backend.log("parity blobs created successfully (in %s)", time.Since(start))
	return nil
}

// Put save a new blob, hash must be the blake2b hash hex-encoded of the data.
//
// If the blob is already stored, then Put will be a no-op.
// So it's not necessary to make call Exists before saving a new blob.
func (backend *BlobsFiles) Put(hash string, data []byte) (err error) {
	// Acquire the lock
	backend.Lock()
	defer backend.Unlock()

	backend.wg.Add(1)
	defer backend.wg.Done()

	// Check if any async error is stored
	if err := backend.lastError(); err != nil {
		return err
	}

	// Ensure the data is not already stored
	exists, err := backend.index.checkPos(hash)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}

	// Encode the blob
	blobSize, blobEncoded := backend.encodeBlob(data, flagBlob)

	var newBlobsFileNeeded bool

	// Ensure the blosfile size won't exceed the maxBlobsFileSize
	if backend.size+int64(blobSize+blobOverhead) > backend.maxBlobsFileSize {
		var f *os.File
		f = backend.current
		backend.current = nil
		newBlobsFileNeeded = true

		// This goroutine will write the parity blobs and close the file
		go func(f *os.File, size int, n int) {
			// Write some parity blobs at the end of the blobsfile using Reed-Solomon erasure coding
			if err := backend.writeParityBlobs(f, size); err != nil {
				backend.setLastError(err)
			}
			if backend.blobsFilesSealedFunc != nil {
				backend.blobsFilesSealedFunc(backend.filename(n))
			}
		}(f, int(backend.size), backend.n)
	}

	if newBlobsFileNeeded {
		// Archive this blobsfile, start by creating a new one
		backend.n++
		if err := backend.wopen(backend.n); err != nil {
			panic(err)
		}
		// Re-open it (since we may need to read blobs from it)
		if err := backend.ropen(backend.n); err != nil {
			panic(err)
		}
		// Update the number of blobsfiles in the index
		if err := backend.saveN(); err != nil {
			panic(err)
		}
	}

	// Save the blob in the BlobsFile
	offset := backend.size
	n, err := backend.current.Write(blobEncoded)
	backend.size += int64(len(blobEncoded))
	if err != nil || n != len(blobEncoded) {
		panic(err)
	}

	// Fsync
	if err = backend.current.Sync(); err != nil {
		panic(err)
	}

	// Save the blob in the index
	blobPos := &blobPos{n: backend.n, offset: offset, size: blobSize, blobSize: len(data)}
	if err := backend.index.setPos(hash, blobPos); err != nil {
		panic(err)
	}

	// Update the expvars
	bytesUploaded.Add(backend.directory, int64(len(blobEncoded)))
	blobsUploaded.Add(backend.directory, 1)
	return
}

// Exists return true if the blobs is already stored.
func (backend *BlobsFiles) Exists(hash string) (bool, error) {
	res, err := backend.index.checkPos(hash)
	if err != nil {
		return false, err
	}

	return res, nil
}

func (backend *BlobsFiles) decodeBlob(data []byte) (size int, blob []byte, flag byte) {
	flag = data[hashSize]
	// checkFlag(flag)
	compressionAlgFlag := CompressionAlgorithm(data[hashSize+1])

	size = int(binary.LittleEndian.Uint32(data[hashSize+2 : blobOverhead]))

	blob = make([]byte, size)
	copy(blob, data[blobOverhead:])

	var blobDecoded []byte
	var err error
	switch compressionAlgFlag {
	case 0:
	case Snappy:
		blobDecoded, err = snappy.Decode(blobDecoded, blob)
		if err != nil {
			panic(fmt.Errorf("failed to decode blob with Snappy: %v", err))
		}
		flag = flagBlob
		blob = blobDecoded
	}

	h, err := blake2b.New256(nil)
	if err != nil {
		panic(err)
	}
	h.Write(blob)

	if !bytes.Equal(h.Sum(nil), data[0:hashSize]) {
		panic(fmt.Errorf("hash doesn't match %x != %x", h.Sum(nil), data[0:hashSize]))
	}

	return
}

func makeHeaderEOF(padSize int64) (h []byte) {
	// Write a hash with only zeroes
	h = make([]byte, blobOverhead)
	// EOF flag, empty second flag
	h[32] = flagEOF
	binary.LittleEndian.PutUint32(h[34:], uint32(padSize))
	return
}

func (backend *BlobsFiles) encodeBlob(blob []byte, flag byte) (size int, data []byte) {
	h, err := blake2b.New256(nil)
	if err != nil {
		panic(err)
	}
	h.Write(blob)

	var compressionAlgFlag byte
	// Only compress regular blobs
	if flag == flagBlob && backend.compression != 0 {
		var dataEncoded []byte
		switch backend.compression {
		case 0:
		case Snappy:
			dataEncoded = snappy.Encode(nil, blob)
			compressionAlgFlag = byte(Snappy)
		}
		flag = flagCompressed
		blob = dataEncoded
	}

	size = len(blob)
	data = make([]byte, len(blob)+blobOverhead)

	copy(data[:], h.Sum(nil))

	// set the flag
	data[hashSize] = flag
	data[hashSize+1] = compressionAlgFlag

	binary.LittleEndian.PutUint32(data[hashSize+2:], uint32(size))

	copy(data[blobOverhead:], blob)

	return
}

// BlobPos return the index entry for the given hash
func (backend *BlobsFiles) blobPos(hash string) (*blobPos, error) {
	return backend.index.getPos(hash)
}

// Size returns the blob size for the given hash.
func (backend *BlobsFiles) Size(hash string) (int, error) {
	if err := backend.lastError(); err != nil {
		return 0, err
	}

	// Fetch the index entry
	blobPos, err := backend.index.getPos(hash)
	if err != nil {
		return 0, fmt.Errorf("error fetching GetPos: %v", err)
	}

	// No index entry found, returns an error
	if blobPos == nil {
		if err == nil {
			return 0, ErrBlobNotFound
		}
		return 0, err
	}

	return blobPos.blobSize, nil
}

// Get returns the blob for the given hash.
func (backend *BlobsFiles) Get(hash string) ([]byte, error) {
	if err := backend.lastError(); err != nil {
		return nil, err
	}

	// Fetch the index entry
	blobPos, err := backend.index.getPos(hash)
	if err != nil {
		return nil, fmt.Errorf("error fetching GetPos: %v", err)
	}

	// No index entry found, returns an error
	if blobPos == nil {
		if err == nil {
			return nil, ErrBlobNotFound
		}
		return nil, err
	}

	// Read the encoded blob from the BlobsFile
	data := make([]byte, blobPos.size+blobOverhead)
	n, err := backend.files[blobPos.n].ReadAt(data, int64(blobPos.offset))
	if err != nil {
		return nil, fmt.Errorf("error reading blob: %v / blobsfile: %+v", err, backend.files[blobPos.n])
	}

	// Ensure the data length is expcted
	if n != blobPos.size+blobOverhead {
		return nil, fmt.Errorf("error reading blob %v, read %v, expected %v+%v", hash, n, blobPos.size, blobOverhead)
	}

	// Decode the blob
	blobSize, blob, _ := backend.decodeBlob(data)
	if blobSize != blobPos.size {
		return nil, fmt.Errorf("bad blob %v encoded size, got %v, expected %v", hash, n, blobSize)
	}

	// Update the expvars
	bytesDownloaded.Add(backend.directory, int64(blobSize))
	blobsUploaded.Add(backend.directory, 1)

	return blob, nil
}

// Enumerate outputs all the blobs into the given chan (ordered lexicographically).
func (backend *BlobsFiles) Enumerate(blobs chan<- *Blob, start, end string, limit int) error {
	defer close(blobs)
	backend.Lock()
	defer backend.Unlock()

	if err := backend.lastError(); err != nil {
		return err
	}

	s, err := hex.DecodeString(start)
	if err != nil {
		return err
	}

	// Enumerate the raw index directly
	endBytes := []byte(end)
	enum := backend.index.db.Range(formatKey(blobPosKey, s), endBytes, false)
	defer enum.Close()
	k, _, err := enum.Next()

	i := 0
	for ; err == nil; k, _, err = enum.Next() {

		if limit != 0 && i == limit {
			return nil
		}

		hash := hex.EncodeToString(k[1:])
		blobPos, err := backend.blobPos(hash)
		if err != nil {
			return nil
		}

		// Remove the BlobPosKey prefix byte
		blobs <- &Blob{
			Hash: hash,
			Size: blobPos.blobSize,
			N:    blobPos.n,
		}

		i++
	}

	return nil
}

// Enumerate outputs all the blobs into the given chan (ordered lexicographically).
func (backend *BlobsFiles) EnumeratePrefix(blobs chan<- *Blob, prefix string, limit int) error {
	defer close(blobs)
	backend.Lock()
	defer backend.Unlock()

	if err := backend.lastError(); err != nil {
		return err
	}

	s, err := hex.DecodeString(prefix)
	if err != nil {
		return err
	}

	// Enumerate the raw index directly
	enum := backend.index.db.PrefixRange(formatKey(blobPosKey, s), false)
	defer enum.Close()
	k, _, err := enum.Next()

	i := 0
	for ; err == nil; k, _, err = enum.Next() {

		if limit != 0 && i == limit {
			return nil
		}

		hash := hex.EncodeToString(k[1:])
		blobPos, err := backend.blobPos(hash)
		if err != nil {
			return nil
		}

		// Remove the BlobPosKey prefix byte
		blobs <- &Blob{
			Hash: hash,
			Size: blobPos.blobSize,
			N:    blobPos.n,
		}

		i++
	}

	return nil
}
