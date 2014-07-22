package clientutil

import (
	"fmt"
	"strconv"
	"sync"

	"github.com/dchest/blake2b"
)

var metaPool = sync.Pool{
	New: func() interface{} { return &Meta{} },
}

type Meta struct {
	Name    string `redis:"name"`
	Type    string `redis:"type"`
	Size    int    `redis:"size"`
	Mode    uint32 `redis:"mode"`
	ModTime string `redis:"mtime"`
	Ref     string `redis:"ref"`
	Hash    string `redis:"-"`
}

func (m *Meta) free() {
	m.Name = ""
	m.Type = ""
	m.Size = 0
	m.Mode = 0
	m.ModTime = ""
	m.Ref = ""
	m.Hash = ""
	metaPool.Put(m)
}

func (m *Meta) metaKey() string {
	hash := blake2b.New256()
	hash.Write([]byte(m.Name))
	hash.Write([]byte(m.Type))
	hash.Write([]byte(strconv.Itoa(int(m.Size))))
	hash.Write([]byte(strconv.Itoa(int(m.Mode))))
	hash.Write([]byte(m.ModTime))
	hash.Write([]byte(m.Ref))
	return fmt.Sprintf("%x", hash.Sum(nil))
}

func NewMeta() *Meta {
	return metaPool.Get().(*Meta)
}

func (m *Meta) ComputeHash() {
	m.Hash = m.metaKey()
	return
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
