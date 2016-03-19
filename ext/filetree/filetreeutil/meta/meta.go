package meta

import (
	"encoding/json"
	"fmt"
	"mime"
	"path/filepath"
	"sync"

	"github.com/dchest/blake2b"
)

var metaPool = sync.Pool{
	New: func() interface{} {
		return &Meta{
			Refs:    []interface{}{},
			Version: "1",
		}
	},
}

type Meta struct {
	Name    string                 `json:"name"`
	Type    string                 `json:"type"`
	Size    int                    `json:"size"`
	Mode    uint32                 `json:"mode"`
	ModTime string                 `json:"mtime"`
	Refs    []interface{}          `json:"refs"`
	Version string                 `json:"version"`
	Extra   map[string]interface{} `json:"extra,omitempty"`
	Hash    string                 `json:"-"`
}

func (m *Meta) free() {
	m.Refs = m.Refs[:0]
	m.Name = ""
	m.Extra = nil
	m.Type = ""
	m.Size = 0
	m.Mode = 0
	m.ModTime = ""
	m.Hash = ""
	m.Version = "1"
	metaPool.Put(m)
}

// JSON returns the `Meta` JSON encoded as (hash, js)
func (m *Meta) Json() (string, []byte) {
	js, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	h := fmt.Sprintf("%x", blake2b.Sum256(js))
	return h, js
}

func (m *Meta) AddIndexedRef(index int, hash string) {
	m.Refs = append(m.Refs, []interface{}{index, hash})
}

func (m *Meta) AddRef(hash string) {
	m.Refs = append(m.Refs, hash)
}

func NewMeta() *Meta {
	return metaPool.Get().(*Meta)
}

func NewMetaFromBlob(hash string, blob []byte) (*Meta, error) {
	meta := NewMeta()
	if err := json.Unmarshal(blob, meta); err != nil {
		return nil, err
	}
	meta.Hash = hash
	return meta, nil
}

func (m *Meta) ContentType() string {
	if m.IsFile() {
		return mime.TypeByExtension(filepath.Ext(m.Name))
	}
	return ""
}

// IsFile returns true if the Meta is a file.
func (m *Meta) IsFile() bool {
	if m.Type == "file" {
		return true
	}
	return false
}

// IsDir returns true if the Meta is a directory.
func (m *Meta) IsDir() bool {
	if m.Type == "dir" {
		return true
	}
	return false
}

func (m *Meta) Close() {
	m.free()
	m = nil
}
