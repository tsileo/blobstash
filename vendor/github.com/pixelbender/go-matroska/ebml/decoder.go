package ebml

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
	"reflect"
	"time"
)

const (
	bufferSize    = 64000
	maxBufferSize = 64000
)

type DecodeOptions struct {
	SkipDamaged   bool
	DecodeUnknown func(id uint32, elem *Reader) error
}

func NewReader(r io.Reader, opt *DecodeOptions) *Reader {
	seek, _ := r.(io.ReadSeeker)
	return &Reader{
		dec: &decoderState{
			opt:  opt,
			buf:  make([]byte, bufferSize),
			seek: seek,
			src:  r,
		},
		len: -1,
	}
}

func NewReaderBytes(b []byte, opt *DecodeOptions) *Reader {
	return &Reader{
		dec: &decoderState{
			opt: opt,
			buf: b,
		},
		len: int64(len(b)),
	}
}

type Reader struct {
	dec *decoderState
	sub *Reader
	len int64
}

// Read reads the EBML-encoded element bytes into b.
func (r *Reader) Read(b []byte) (int, error) {
	if err := r.skip(); err != nil {
		return 0, err
	}
	if r.len < 0 {
		return r.dec.Read(b)
	}
	if r.len < int64(len(b)) {
		b = b[:int(r.len)]
	}
	n, err := r.dec.Read(b)
	r.len -= int64(n)
	return n, err
}

// Decode reads the next EBML-encoded value from its input and stores it in the value pointed to by v.
func (r *Reader) Decode(v interface{}) error {
	if u, ok := v.(Unmarshaler); ok {
		return u.UnmarshalEBML(r)
	}
	if v == nil {
		return errors.New("ebml: decode nil")
	}
	p := reflect.ValueOf(v)
	if p.Kind() != reflect.Ptr {
		return errors.New("ebml: decode not a pointer")
	}
	return unmarshal(r, p, r.dec.opt)
}

// ReadElement reads the next EMBL-encoded element ID and size
func (r *Reader) ReadElement() (id uint32, elem *Reader, err error) {
	id, err = r.readID()
	if err != nil {
		return
	}
	elem, err = r.readElement()
	r.sub = elem
	return
}

// ReadString reads and returns a UTF-8 encoded EBML string value.
func (r *Reader) ReadString() (string, error) {
	if r.len < 0 {
		return "", errFormat("string")
	}
	if r.len > maxBufferSize {
		return "", errors.New("ebml: string length too large")
	}
	b, err := r.next(int(r.len))
	if err != nil {
		return "", err
	}
	i := len(b)
	for i > 0 && b[i-1] == 0 {
		i--
	}
	return string(b[:i]), nil
}

// Len returns remaining bytes length of the Element.
// Returns -1 if length is not known.
func (r *Reader) Len() int64 {
	return r.len
}

// ReadFloat reads and returns a EBML int value.
func (r *Reader) ReadInt() (int64, error) {
	if r.len < 0 || r.len > 8 {
		return 0, errFormat("int")
	}
	b, err := r.next(int(r.len))
	if err != nil {
		return 0, err
	}
	v := int64(0)
	for _, it := range b {
		v = (v << 8) | int64(it)
	}
	return v, nil
}

// ReadFloat reads and returns a EBML boolean value.
func (r *Reader) ReadBool() (bool, error) {
	v, err := r.ReadInt()
	return v != 0, err
}

// ReadFloat reads and returns a EBML float value.
func (r *Reader) ReadFloat() (float64, error) {
	switch r.len {
	case 4:
		b, err := r.next(4)
		if err != nil {
			return 0, err
		}
		return float64(math.Float32frombits(binary.BigEndian.Uint32(b))), nil
	case 8:
		b, err := r.next(8)
		if err != nil {
			return 0, err
		}
		return math.Float64frombits(binary.BigEndian.Uint64(b)), nil
	default:
		return 0, errFormat("float")
	}
}

// ReadTime reads and returns a EBML time value.
func (r *Reader) ReadTime() (time.Time, error) {
	v, err := r.ReadInt()
	return timeAbs.Add(time.Duration(v) * time.Nanosecond), err
}

func (r *Reader) Next(n int) ([]byte, error) {
	return r.next(n)
}

func (r *Reader) next(n int) ([]byte, error) {
	if err := r.skip(); err != nil {
		return nil, err
	}
	if r.len < 0 {
		return r.dec.Next(n)
	}
	if r.len == 0 {
		return nil, io.EOF
	}
	if n < 0 || r.len < int64(n) {
		return nil, errFormat("length")
	}
	b, err := r.dec.Next(n)
	r.len -= int64(n)
	return b, err
}

func (r *Reader) skip() error {
	if r.sub == nil {
		return nil
	}
	s, v := r.sub, int64(0)
	for s != nil {
		if s.len > 0 {
			v += s.len
		}
		s = s.sub
	}
	r.sub = nil
	return r.dec.Skip(v)
}

func (r *Reader) readID() (uint32, error) {
	b, err := r.next(1)
	for err == nil && b[0] < 0x10 {
		// Skip incomplete elements
		b, err = r.next(1)
	}
	if err != nil {
		return 0, err
	}
	n, v, bit := 0, b[0], uint8(0x80)
	for bit > 0xf && v&bit == 0 {
		n++
		bit >>= 1
	}
	if n > 3 {
		return 0, errFormat("id")
	}
	id := uint32(v)
	if n > 0 {
		if b, err = r.next(n); err != nil {
			return 0, err
		}
		for _, v := range b {
			id = (id << 8) | uint32(v)
		}
	}
	return id, nil
}

func (r *Reader) ReadVInt() (int64, error) {
	b, err := r.next(1)
	if err != nil {
		return 0, err
	}
	n, v, bit := 0, b[0], uint8(0x80)
	for bit > 0 && v&bit == 0 {
		n++
		bit >>= 1
	}
	bit--
	i := int64(v & bit)
	if n > 0 {
		if b, err = r.next(n); err != nil {
			return 0, err
		}
		for _, v := range b {
			i = (i << 8) | int64(v)
		}
	}
	return i, nil
}

func (r *Reader) readElement() (*Reader, error) {
	b, err := r.next(1)
	if err != nil {
		return nil, err
	}
	n, v, bit := 0, b[0], uint8(0x80)
	for bit > 0 && v&bit == 0 {
		n++
		bit >>= 1
	}
	if n > 7 {
		return nil, errFormat("size")
	}
	bit--
	size := int64(v & bit)
	mask := v | ^bit
	if n > 0 {
		if b, err = r.next(n); err != nil {
			return nil, err
		}
		for _, v := range b {
			mask &= v
			size = (size << 8) | int64(v)
		}
	}
	if mask == 0xff {
		// Unknown element size
		return &Reader{r.dec, nil, -1}, nil
	}
	if r.len >= 0 {
		if r.len < size {
			return nil, io.ErrUnexpectedEOF
		}
		r.len -= size
	}
	return &Reader{r.dec, nil, size}, nil
}

type decoderState struct {
	opt  *DecodeOptions
	seek io.ReadSeeker
	src  io.Reader
	buf  []byte
	r, w int
	off  int64
}

func (s *decoderState) Offset() int64 {
	return s.off
}

func (s *decoderState) Next(n int) ([]byte, error) {
	if len(s.buf) < n {
		return nil, errors.New("ebml: buffer too small")
	}
	if p := n - s.w + s.r; p > 0 {
		if err := s.fill(p); err != nil {
			return nil, err
		}
	}
	b := s.buf[s.r : s.r+n]
	s.r += n
	s.off += int64(n)
	return b, nil
}

func (s *decoderState) Read(b []byte) (int, error) {
	if s.w-s.r >= len(b) {
		s.r += copy(b, s.buf[s.r:])
		s.off += int64(len(b))
		return len(b), nil
	}
	p := 0
	if s.w > s.r {
		p = copy(b, s.buf[s.r:s.w])
		s.r, s.w = 0, 0
		b = b[p:]
	}
	if len(b) > len(s.buf) {
		n, err := io.ReadFull(s.src, b)
		s.off += int64(p + n)
		return p + n, err
	}
	if err := s.fill(len(b)); err != nil {
		// TODO: partial read....
		s.off += int64(p)
		return p, err
	}
	s.r += copy(b, s.buf[s.r:])
	s.off += int64(p + len(b))
	return p + len(b), nil
}

func (s *decoderState) Skip(n int64) error {
	d := int64(s.w - s.r)
	if d >= n {
		s.r += int(n)
		s.off += n
		return nil
	}
	if d > 0 {
		n -= d
		s.off += d
		s.r, s.w = 0, 0
	}
	if s.seek != nil {
		_, err := s.seek.Seek(n, io.SeekCurrent)
		if err != nil {
			return err
		}
		s.off += n
		return nil
	}
	for n > 0 {
		p := len(s.buf)
		if n < int64(p) {
			p = int(n)
		}
		m, err := io.ReadAtLeast(s.src, s.buf, p)
		if err != nil {
			return err
		}
		n -= int64(p)
		s.off += int64(p)
		s.r, s.w = p, m
	}
	return nil
}

func (s *decoderState) fill(n int) error {
	if s.r > 0 {
		s.w = copy(s.buf, s.buf[s.r:s.w])
		s.r = 0
	}
	p, err := io.ReadAtLeast(s.src, s.buf[s.w:], n)
	if err != nil {
		return err
	}
	s.w += p
	return nil
}

type errFormat string

func (e errFormat) Error() string {
	return "ebml: " + string(e) + " format error"
}
