package ebml

import (
	"errors"
	"io"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Unmarshaler is the interface implemented by objects that can unmarshal
// a EBML description of themselves. UnmarshalEBML must copy the EBML data
// if it wishes to retain the data after returning.
type Unmarshaler interface {
	UnmarshalEBML(r *Reader) error
}

// Marshaler is the interface implemented by objects that can marshal themselves into valid EBML.
// type Marshaler interface {
// 	MarshalEBML(enc *Encoder) error
// }

// func marshal(w *Writer, v reflect.Value, opt *EncodeOptions) error {
// }

func unmarshal(r *Reader, v reflect.Value, opt *DecodeOptions) error {
	switch v.Kind() {
	case reflect.Struct:
		t := v.Type()
		switch t {
		case timeType:
			t, err := r.ReadTime()
			if err != nil {
				return err
			}
			v.Set(reflect.ValueOf(t))
		default:
			s, err := getStructMapping(t)
			if err != nil {
				return err
			}
			if err = s.setDefaults(v); err != nil {
				return err
			}
			if u, ok := v.Interface().(Unmarshaler); ok {
				return u.UnmarshalEBML(r)
			}
			return s.unmarshal(r, v, opt)
		}
	case reflect.Ptr:
		e := v.Type().Elem()
		if v.IsNil() {
			v.Set(reflect.New(e))
		}
		if u, ok := v.Interface().(Unmarshaler); ok {
			return u.UnmarshalEBML(r)
		}
		return unmarshal(r, v.Elem(), opt)
	case reflect.Slice:
		e := v.Type().Elem()
		switch e.Kind() {
		case reflect.Uint8:
			b, err := r.next(int(r.len)) // TODO: limit
			if err != nil {
				return err
			}
			v.SetBytes(b)
		default:
			n := v.Len()
			v.Set(reflect.Append(v, reflect.Zero(e)))
			if err := unmarshal(r, v.Index(n), opt); err != nil {
				v.SetLen(n)
				return err
			}
			return nil
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		i, err := r.ReadInt()
		if err != nil {
			return err
		}
		v.SetInt(i)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		i, err := r.ReadInt()
		if err != nil {
			return err
		}
		v.SetUint(uint64(i))
	case reflect.Bool:
		b, err := r.ReadBool()
		if err != nil {
			return err
		}
		v.SetBool(b)
	case reflect.Float32, reflect.Float64:
		f, err := r.ReadFloat()
		if err != nil {
			return err
		}
		v.SetFloat(f)
	case reflect.String:
		s, err := r.ReadString()
		if err != nil {
			return err
		}
		v.SetString(s)
	case reflect.Interface:
		if !v.IsNil() {
			return unmarshal(r, v.Elem(), opt)
		}
	default:
		return &errUnmarshal{v.Type()}
	}
	return nil
}

type errUnmarshal struct {
	t reflect.Type
}

func (e *errUnmarshal) Error() string {
	return "ebml: can not unmarshal " + e.t.String()
}

type structMapping struct {
	fields []*field
	ids    map[uint32]*field
}

func newStructMapping(t reflect.Type) (*structMapping, error) {
	if t.Kind() != reflect.Struct {
		return nil, errors.New("ebml: Decode not a struct")
	}
	n := t.NumField()
	fields := make([]*field, 0, n)
	ids := make(map[uint32]*field)

	for i := 0; i < n; i++ {
		p := t.Field(i)
		if p.PkgPath != "" && !p.Anonymous { // TODO: implement
			continue
		}
		tag := p.Tag.Get("ebml")
		if tag == "" || tag == "-" {
			continue
		}
		f, err := newField(p, i, tag)
		if err != nil {
			return nil, &errStructMapping{t, p.Name, err}
		}
		fields = append(fields, f)
		ids[f.id] = f
	}
	return &structMapping{fields, ids}, nil
}

type errStructMapping struct {
	t    reflect.Type
	name string
	err  error
}

func (e *errStructMapping) Error() string {
	return "ebml: " + e.name + " of " + e.t.String() + " mapping: " + e.err.Error()
}

func (m *structMapping) setDefaults(v reflect.Value) error {
	for _, it := range m.fields {
		if it.def != nil {
			v.Field(it.index).Set(*it.def)
		}
	}
	return nil
}

func (m *structMapping) unmarshal(r *Reader, v reflect.Value, opt *DecodeOptions) error {
	for {
		id, elem, err := r.ReadElement()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if f, ok := m.ids[id]; ok {
			err = f.unmarshal(elem, v.Field(f.index), opt)
		} else if opt.DecodeUnknown != nil {
			err = opt.DecodeUnknown(id, elem)
		}
		if err != nil {
			if opt.SkipDamaged {
				continue
			}
			return err
		}
	}
	return nil
}

type field struct {
	id        uint32
	seq       []uint32
	index     int
	name      string
	def       *reflect.Value
	omitempty bool
}

func newField(t reflect.StructField, index int, tag string) (*field, error) {
	v := strings.Split(tag, ",")
	seq := strings.Split(v[0], ">")
	// TODO: omitempty

	f := &field{
		index: index,
		name:  t.Name,
	}
	for i, s := range seq {
		id, err := strconv.ParseUint(s, 16, 32)
		if err != nil {
			return nil, err
		}
		if i == 0 {
			f.id = uint32(id)
		} else {
			f.seq = append(f.seq, uint32(id))
		}
	}
	for _, it := range v[1:] {
		switch it {
		case "omitempty":
			f.omitempty = true
		default:
			def, err := newDefault(t.Type, it)
			if err != nil {
				return nil, err
			}
			v := reflect.ValueOf(def).Convert(t.Type)
			f.def = &v
		}
	}

	return f, nil
}

func newDefault(t reflect.Type, v string) (interface{}, error) {
	switch t.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.ParseInt(v, 10, int(t.Size())*8)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return strconv.ParseUint(v, 10, int(t.Size())*8)
	case reflect.Bool:
		return strings.ToLower(v) != "false", nil
	case reflect.Float32, reflect.Float64:
		return strconv.ParseFloat(v, int(t.Size())*8)
	case reflect.String:
		return v, nil
	default:
		return nil, errors.New("default value is not supported")
	}
}

func (f *field) unmarshal(r *Reader, v reflect.Value, opt *DecodeOptions) error {
	if len(f.seq) > 0 {
		return f.unmarshalSeq(r, v, opt, f.seq)
	}
	return unmarshal(r, v, opt)
}

func (f *field) unmarshalSeq(r *Reader, v reflect.Value, opt *DecodeOptions, seq []uint32) error {
	for {
		id, elem, err := r.ReadElement()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if id == seq[0] {
			if len(seq) > 1 {
				err = f.unmarshalSeq(elem, v, opt, seq[1:])
			} else {
				err = unmarshal(elem, v, opt)
			}
		} else if opt.DecodeUnknown != nil {
			err = opt.DecodeUnknown(id, elem)
		}
		if err != nil {
			if opt.SkipDamaged {
				continue
			}
			return err
		}
	}
	return nil
}

var timeType = reflect.TypeOf(time.Time{})
var timeAbs = time.Date(2001, time.January, 1, 0, 0, 0, 0, time.UTC)

var (
	mappingCacheLock sync.RWMutex
	mappingCache     = make(map[reflect.Type]*structMapping)
)

func getStructMapping(t reflect.Type) (*structMapping, error) {
	m := &mappingCacheLock
	m.RLock()
	s, ok := mappingCache[t]
	if m.RUnlock(); ok {
		return s, nil
	}
	m.Lock()
	defer m.Unlock()
	if s, ok = mappingCache[t]; ok {
		return s, nil
	}
	s, err := newStructMapping(t)
	if err != nil {
		return nil, err
	}
	mappingCache[t] = s
	return s, nil
}
