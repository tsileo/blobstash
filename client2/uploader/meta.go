package uploader

import (
	"crypto/sha1"
	"fmt"
	"strconv"
	"sync"
)

var metaPool = sync.Pool{
	New: func() interface{} { return &Meta{} },
}

type Meta struct {
	Name string `redis:"name"`
	Type string `redis:"type"`
	Size int    `redis:"size"`
	Mode uint32 `redis:"mode"`
	ModTime string `redis:"mtime"`
	Ref  string `redis:"ref"`
	Hash string `redis:"-"`
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
	sha := sha1.New()
	sha.Write([]byte(m.Name))
	sha.Write([]byte(m.Type))
	sha.Write([]byte(strconv.Itoa(int(m.Size))))
	sha.Write([]byte(strconv.Itoa(int(m.Mode))))
	sha.Write([]byte(m.ModTime))
	sha.Write([]byte(m.Ref))
	return fmt.Sprintf("%x", sha.Sum(nil))
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
