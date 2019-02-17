package ebml

import (
// "errors"
// "io"
// "reflect"
)

/*
type EncodeOptions struct {
}

type Encoder struct {
	w *Writer
}

func NewEncoder(w io.Writer, opt *EncodeOptions) *Encoder {
	seek,_ := w.(io.WriteSeeker)
	dec.w = &Writer{
		dec: &encoderState{
			buf: make([]byte, bufferSize),
			seek: seek,
			src: r,
			opt: opt,
		},
		len: -1,
	}
	return dec
}

// Encode writes the EBML encoding of v to the stream.
func (enc *Encoder) Flush() (err error) {
	enc.pack()
	enc.render()

	enc.flush(w)
	// TODO: flush
	enc.elem = nil
	return nil
}

type Writer struct {
	*encoderState
}

type encoderState struct {
}

// An Encoder writes EBML elements to an output stream.
type Encoder struct {
	w io.Writer

	id   int64
	size int
	buf  []byte
	elem []*Encoder
}

// NewEncoder returns a new encoder that writes to w.
func NewEncoder(w io.Writer) *Encoder {
	enc := new(Encoder)
	enc.w = w
	return enc
}

// Encode writes the EBML encoding of v to the stream.
func (enc *Encoder) Encode(v interface{}) (err error) {
	if u, ok := v.(Marshaler); ok {
		return u.MarshalEBML(enc)
	}
	if v == nil {
		return errors.New("ebml: Encode nil")
	}
	ref := reflect.ValueOf(v)
	if ref = ref.Elem(); ref.Kind() != reflect.Struct {
		return errors.New("ebml: Decode not a struct")
	}
	codec := &typeCodec{ref}
	return codec.MarshalEBML(enc)
}



func (enc *Encoder) pack() int64 {
	for _, it := range enc.elem {
		enc.size += it.pack()
	}
	return enc.size
}

func (enc *Encoder) render() {
	if enc.id != 0 {

	}
	for _, it := range enc.elem {
		it.render()
	}
}

// NewElement writes the EBML element of v to the stream, adding outermost element header.
func (enc *Encoder) NewElement(id int64) *Encoder {
	e := &Encoder{id: id}
	enc.elem = append(enc.elem, e)
	return e
}

func (enc *Encoder) WriteInt(id int64, v int64) {
	// TODO: optimized version of byte write
	var b []byte
	pos := 0
	for i, off := range intOffset {
		it := byte(v >> off)
		if it != 0 {
			if b == nil {
				b = enc.next(8 - i)
			}
			b[pos] = it
			pos++
		}
	}
}

func (enc *Encoder) WriteData(id int64, v []byte) {

}

func (enc *Encoder) WriteString(id int64, v string) {

}

func (enc *Encoder) writeVint(id int64, v int64) {

}

func (enc *Encoder) next(n int) (b []byte) {
	off := enc.size + n
	if m := len(enc.buf); m < off {
		if m = off; m < 64 {
			m = 64
		} else {
			m <<= 1
		}
		b = make([]byte, m)
		copy(b, enc.buf[:enc.size])
		enc.buf = b
	}
	b, enc.size = enc.buf[enc.size:off], off
	return
}

func (dec *Decoder) readVsint(off int) (v int64, n int, err error) {
	m, err := dec.buf.ReadByte()
	if err != nil {
		return
	}
	dec.len--
	var bit byte
	for n, bit = range mask {
		if m&bit != 0 {
			v = int64(m & rest[n+off])
			break
		}
	}
	if n > 0 {
		if dec.len < int64(n) && 0 < dec.size {
			err = io.EOF
			return
		}
		var b []byte
		if b, err = dec.buf.Peek(n); err != nil {
			return
		}
		for _, it := range b {
			v = (v << 8) | int64(it)
		}
		if _, err = dec.buf.Discard(n); err != nil {
			return
		}
		dec.len -= int64(n)
	}
	return
}

*/
