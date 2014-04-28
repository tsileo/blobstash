package models

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
