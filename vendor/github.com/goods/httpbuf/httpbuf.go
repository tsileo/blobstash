package httpbuf

import (
	"bytes"
	"net/http"
	"sync"
)

//Buffer is a type that implements http.ResponseWriter but buffers all the data
//and headers.
type Buffer struct {
	bytes.Buffer
	resp    int
	headers http.Header
	once    sync.Once
}

//Header implements the header method of http.ResponseWriter
func (b *Buffer) Header() http.Header {
	b.once.Do(func() {
		b.headers = make(http.Header)
	})
	return b.headers
}

//WriteHeader implements the WriteHeader method of http.ResponseWriter
func (b *Buffer) WriteHeader(resp int) {
	b.resp = resp
}

//Apply takes an http.ResponseWriter and calls the required methods on it to
//output the buffered headers, response code, and data. It returns the number
//of bytes written and any errors flushing.
func (b *Buffer) Apply(w http.ResponseWriter) (n int, err error) {
	if len(b.headers) > 0 {
		h := w.Header()
		for key, val := range b.headers {
			h[key] = val
		}
	}
	if b.resp > 0 {
		w.WriteHeader(b.resp)
	}
	n, err = w.Write(b.Bytes())
	return
}
