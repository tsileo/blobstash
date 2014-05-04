package client

import (
	"os"
	"crypto/sha1"
	"bufio"
	"fmt"
	"io"
)

func SHA1(data []byte) string {
	h := sha1.New()
	h.Write(data)
	return fmt.Sprintf("%x", h.Sum(nil))
}

func FullSHA1(path string) string {
	f, _ := os.Open(path)
	defer f.Close()
	reader := bufio.NewReader(f)
	h := sha1.New()
	_, _ = io.Copy(h, reader)
	return fmt.Sprintf("%x", h.Sum(nil))
}

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

func (wr *WriteResult) Add(wr2 *WriteResult) {
	wr.Size += wr2.Size
	wr.BlobsCnt += wr2.BlobsCnt
	wr.SkippedCnt += wr2.SkippedCnt
	wr.SkippedSize += wr2.SkippedSize
	wr.UploadedCnt += wr2.UploadedCnt
	wr.UploadedSize += wr2.UploadedSize
}

type ReadResult struct {
	Hash string
	Size int
	BlobsCnt int
//	SkippedCnt int
//	SkippedSize int
	DownloadedCnt int
	DownloadedSize int
}

func (rr *ReadResult) Add(rr2 *ReadResult) {
	rr.Size += rr2.Size
	rr.BlobsCnt += rr2.BlobsCnt
//	rr.SkippedCnt += rr2.SkippedCnt
//	rr.SkippedSize += rr2.SkippedSize
	rr.DownloadedCnt += rr2.DownloadedCnt
	rr.DownloadedSize += rr2.DownloadedSize
}

func MatchResult(wr *WriteResult, rr *ReadResult) bool {
	if wr.Size == rr.Size && wr.Hash == rr.Hash &&
	   		wr.BlobsCnt == rr.BlobsCnt &&
	   		(wr.SkippedCnt + wr.UploadedCnt) == rr.DownloadedCnt &&
	   		(wr.SkippedSize + wr.UploadedSize) == rr.DownloadedSize {
	   	return true
	}
	return false
}