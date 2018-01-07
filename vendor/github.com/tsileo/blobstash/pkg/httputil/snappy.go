package httputil

import (
	"io"
	"net/http"
	"sync"

	"github.com/golang/snappy"
)

var (
	snappyWriterPool sync.Pool
)

type snappyResponseWriter struct {
	snappyWriter *snappy.Writer
	rw           http.ResponseWriter
	w            io.Writer
}

// NewSnappyResponseWriter returns a `http.ResponseWriter` wrapper which can encode
// the output with Snappy if requested by the client.
// If snappy isn't enabled, it will act like a regular `http.ResponseWriter`
// `Close` must be call so the `*snappy.Writer` instance can be put back in the `sync.Pool`
func NewSnappyResponseWriter(rw http.ResponseWriter, r *http.Request) *snappyResponseWriter {
	var s *snappy.Writer

	// Set the necessary `Vary` header
	rw.Header().Set("Vary", "Accept-Encoding")
	// Disable caching of responses.
	rw.Header().Set("Cache-Control", "no-cache")

	var writer io.Writer

	switch r.Header.Get("Accept-Encoding") {
	case "snappy":
		rw.Header().Set("Content-Encoding", "snappy")
		// Try to get a snappy.Writer from the pool
		if is := snappyWriterPool.Get(); is != nil {
			s = is.(*snappy.Writer)
			s.Reset(rw)
		} else {
			// Creates a new one if the pool is empty
			s = snappy.NewWriter(rw)
		}
		writer = s
	default:
		// No `Accept-Encoding` header (or unsupported encoding)
		// Default to plain-text
		writer = rw
	}

	return &snappyResponseWriter{
		snappyWriter: s,
		rw:           rw,
		w:            writer,
	}
}

// Header is necessary for satisfying the `http.ResponseWriter` interface.
func (srw *snappyResponseWriter) Header() http.Header {
	return srw.rw.Header()
}

// Write implements io.Writer
func (srw *snappyResponseWriter) Write(b []byte) (int, error) {
	return srw.w.Write(b)
}

// WriteHeader is necessary for satisfying the `http.ResponseWriter` interface.
func (srw *snappyResponseWriter) WriteHeader(status int) {
	srw.rw.WriteHeader(status)
}

// Close put the `snappyWriter` back into the pool
func (srw *snappyResponseWriter) Close() {
	if srw.snappyWriter != nil {
		snappyWriterPool.Put(srw.snappyWriter)
		srw.snappyWriter = nil
	}
}
