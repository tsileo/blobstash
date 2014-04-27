package models

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

type ReadResult struct {
	Hash string
	Size int
	BlobsCnt int
//	SkippedCnt int
//	SkippedSize int
	DownloadedCnt int
	DownloadedSize int
}
