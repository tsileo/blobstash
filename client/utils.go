package client

import (
	"os"
	"crypto/sha1"
	"crypto/rand"
	"bufio"
	"fmt"
	"io"
)

// NewID generate a random hash that can be used as random key
func NewID() string {
	data := make([]byte, 16)
	rand.Read(data)
	return SHA1(data)
}

// SHA1 is a helper to quickly compute the SHA1 hash of a []byte.
func SHA1(data []byte) string {
	h := sha1.New()
	h.Write(data)
	return fmt.Sprintf("%x", h.Sum(nil))
}

// FullSHA1 is helper to compute the SHA1 of the given file path.
func FullSHA1(path string) string {
	f, _ := os.Open(path)
	defer f.Close()
	reader := bufio.NewReader(f)
	h := sha1.New()
	_, _ = io.Copy(h, reader)
	return fmt.Sprintf("%x", h.Sum(nil))
}

// a WriteResult keeps track of the number of blobs uploaded/skipped, and basic infos.
type WriteResult struct {
	Filename string
	Hash string
	Size int
	BlobsCnt int
	SkippedCnt int
	SkippedSize int
	UploadedCnt int
	UploadedSize int
	AlreadyExists bool
}

// Add allows two WriteResult to be added.
func (wr *WriteResult) Add(wr2 *WriteResult) {
	wr.Size += wr2.Size
	wr.BlobsCnt += wr2.BlobsCnt
	wr.SkippedCnt += wr2.SkippedCnt
	wr.SkippedSize += wr2.SkippedSize
	wr.UploadedCnt += wr2.UploadedCnt
	wr.UploadedSize += wr2.UploadedSize
}

// a ReadResult keeps track of the number/size of downloaded blobs.
type ReadResult struct {
	Hash string
	Size int
	BlobsCnt int
//	SkippedCnt int
//	SkippedSize int
	DownloadedCnt int
	DownloadedSize int
}

// Add allow two ReadResult to be added.
func (rr *ReadResult) Add(rr2 *ReadResult) {
	rr.Size += rr2.Size
	rr.BlobsCnt += rr2.BlobsCnt
//	rr.SkippedCnt += rr2.SkippedCnt
//	rr.SkippedSize += rr2.SkippedSize
	rr.DownloadedCnt += rr2.DownloadedCnt
	rr.DownloadedSize += rr2.DownloadedSize
}

// MatchResult checks if a WriteResult and a ReadResult have the same size.
func MatchResult(wr *WriteResult, rr *ReadResult) bool {
	if wr.Size == rr.Size && wr.Hash == rr.Hash &&
	   		wr.BlobsCnt == rr.BlobsCnt &&
	   		(wr.SkippedCnt + wr.UploadedCnt) == rr.DownloadedCnt &&
	   		(wr.SkippedSize + wr.UploadedSize) == rr.DownloadedSize {
	   	return true
	}
	return false
}
