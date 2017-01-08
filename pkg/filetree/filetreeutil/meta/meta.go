/*

Package meta implement the node specification for the filetree extension.

*/
package meta

import (
	"encoding/json"
	"fmt"
	"mime"
	"path/filepath"
	"sync"
	"time"

	"github.com/dchest/blake2b"
)

// XXX(tsileo): think about saving the parent in the `Meta` and if no parent, then, it's a root!

var (
	ModTimeFmt = time.RFC3339
)

var (
	version = "1"
)

var metaPool = sync.Pool{
	New: func() interface{} {
		return &Meta{
			Refs:    []interface{}{},
			Version: version,
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
	Extra   map[string]interface{} `json:"extra,omitempty"` // TODO(tsileo): remove the `Extra` attr from BlobFS and filetree ext
	Data    map[string]interface{} `json:"data,omitempty"`
	XAttrs  map[string]string      `json:"xattrs,omitempty"`
	Hash    string                 `json:"-"`
}

func (m *Meta) free() {
	m.Refs = m.Refs[:0]
	m.Name = ""
	m.Extra = nil
	m.Data = nil
	m.Type = ""
	m.Size = 0
	m.Mode = 0
	m.ModTime = ""
	m.Hash = ""
	m.Version = "1"
	metaPool.Put(m)
}

// AddExtraData insert a new meta data in the Extra field
func (m *Meta) AddExtraData(key string, val interface{}) {
	if m.Extra == nil {
		m.Extra = map[string]interface{}{}
	}
	m.Extra[key] = val
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

func (m *Meta) IsPublic() (public bool) {
	if m.XAttrs != nil {
		// Check if the node is public
		if pub, ok := m.XAttrs["public"]; ok && pub == "1" {
			public = true
		}
	}
	return
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

func (m *Meta) Mtime() (time.Time, error) {
	return time.Parse(time.RFC3339, m.ModTime)
}

func (m *Meta) Close() {
	m.free()
	m = nil
}
