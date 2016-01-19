package clientutil

import (
	"io"
	"net/http"
	"sync"

	"github.com/golang/snappy"
)

var (
	snappyReaderPool sync.Pool
)

type snappyResponseReader struct {
	resp *http.Response
	io.Reader
}

func NewSnappyResponseReader(resp *http.Response) io.ReadCloser {
	var reader io.Reader
	reader = resp.Body
	if resp.Header.Get("Content-Encoding") == "snappy" {
		if isr := snappyReaderPool.Get(); isr != nil {
			sr := isr.(*snappy.Reader)
			sr.Reset(reader)
			reader = sr
		} else {
			// Creates a new one if the pool is empty
			reader = snappy.NewReader(reader)
		}
	}
	return &snappyResponseReader{
		resp:   resp,
		Reader: reader,
	}
}
func (srr *snappyResponseReader) Read(p []byte) (int, error) {
	return srr.Reader.Read(p)
}

func (srr *snappyResponseReader) Close() error {
	if sr, ok := srr.Reader.(*snappy.Reader); ok {
		snappyReaderPool.Put(sr)
		srr.Reader = nil
	}
	return srr.resp.Body.Close()
}
