package reader

type ReadResult struct {
	Hash string
	Size int
	BlobsCnt int
//	SkippedCnt int
//	SkippedSize int
	DownloadedCnt int
	DownloadedSize int
}
