package clientutil

import (
	"bufio"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/dchest/blake2b"
	"github.com/dustin/go-humanize"
)

var wrPool = sync.Pool{
	New: func() interface{} { return &WriteResult{} },
}

// FullHash is helper to compute the Blake2B of the given file path.
func FullHash(path string) string {
	f, _ := os.Open(path)
	defer f.Close()
	reader := bufio.NewReader(f)
	h := blake2b.New256()
	_, _ = io.Copy(h, reader)
	return fmt.Sprintf("%x", h.Sum(nil))
}

// NewID generate a random hash that can be used as random key
func NewID() string {
	data := make([]byte, 16)
	rand.Read(data)
	return fmt.Sprintf("%x", blake2b.Sum256(data))
}

// a WriteResult keeps track of the number of blobs uploaded/skipped, and basic stats.
type WriteResult struct {
	Hash string

	Size         int
	SizeSkipped  int
	SizeUploaded int

	BlobsCount    int
	BlobsSkipped  int
	BlobsUploaded int

	FilesCount    int
	FilesSkipped  int
	FilesUploaded int

	DirsCount    int
	DirsSkipped  int
	DirsUploaded int

	AlreadyExists bool
}

func NewWriteResult() *WriteResult {
	return wrPool.Get().(*WriteResult)
}

func (wr *WriteResult) free() {
	wr.Hash = ""
	wr.Size = 0
	wr.SizeSkipped = 0
	wr.SizeUploaded = 0
	wr.BlobsCount = 0
	wr.BlobsSkipped = 0
	wr.BlobsUploaded = 0
	wr.FilesCount = 0
	wr.FilesSkipped = 0
	wr.FilesUploaded = 0
	wr.DirsCount = 0
	wr.DirsSkipped = 0
	wr.DirsUploaded = 0
	wr.AlreadyExists = false
	wrPool.Put(wr)
}

func (wr *WriteResult) String() string {
	return fmt.Sprintf(`Write Result:
- Size: %v (skipped:%v, uploaded:%v)
- Blobs: %d (skipped:%d, uploaded:%d)
- Files: %d (skipped:%d, uploaded:%d)
- Dirs: %d (skipped:%d, uploaded:%d)
`,
		humanize.Bytes(uint64(wr.Size)), wr.SizeSkipped, wr.SizeUploaded,
		wr.BlobsCount, wr.BlobsSkipped, wr.BlobsUploaded,
		wr.FilesCount, wr.FilesSkipped, wr.FilesUploaded,
		wr.DirsCount, wr.DirsSkipped, wr.DirsUploaded)
}

// Add allows two WriteResult to be added.
func (wr *WriteResult) Add(wr2 *WriteResult) {
	wr.Size += wr2.Size
	wr.SizeSkipped += wr2.SizeSkipped
	wr.SizeUploaded += wr2.SizeUploaded

	wr.BlobsCount += wr2.BlobsCount
	wr.BlobsSkipped += wr2.BlobsSkipped
	wr.BlobsUploaded += wr2.BlobsUploaded

	wr.FilesCount += wr2.FilesCount
	wr.FilesSkipped += wr2.FilesSkipped
	wr.FilesUploaded += wr2.FilesUploaded

	wr.DirsCount += wr2.DirsCount
	wr.DirsSkipped += wr2.DirsSkipped
	wr.DirsUploaded += wr2.DirsUploaded
}

// a ReadResult keeps track of the number/size of downloaded blobs.
type ReadResult struct {
	Hash string

	Size           int
	SizeDownloaded int

	BlobsCount      int
	BlobsDownloaded int

	FilesCount      int
	FilesDownloaded int

	DirsCount      int
	DirsDownloaded int
}

// Add allow two ReadResult to be added.
func (rr *ReadResult) Add(rr2 *ReadResult) {
	rr.Size += rr2.Size
	rr.SizeDownloaded += rr2.SizeDownloaded

	rr.BlobsCount += rr2.BlobsCount
	rr.BlobsDownloaded += rr2.BlobsDownloaded

	rr.FilesCount += rr2.FilesCount
	rr.FilesDownloaded += rr2.FilesDownloaded

	rr.DirsCount += rr2.DirsCount
	rr.DirsDownloaded += rr2.DirsDownloaded
}

// MatchResult checks if a WriteResult and a ReadResult have the same size.
func MatchResult(wr *WriteResult, rr *ReadResult) bool {
	if wr.Size == rr.Size &&
		wr.FilesCount == rr.FilesCount &&
		wr.DirsCount == rr.DirsCount {
		return true
	}
	return false
}
