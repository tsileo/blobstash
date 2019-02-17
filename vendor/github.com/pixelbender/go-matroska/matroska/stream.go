package matroska

import "io"

type Reader struct {
}

func NewReader(r io.Reader) *Reader {
	return &Reader{}
}
