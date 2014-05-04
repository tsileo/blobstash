package backend

import (
	"io/ioutil"
	"os"
	"path/filepath"
)

type LocalBackend struct {
	Directory string
}

func (b *LocalBackend) Put(hash string, data []byte) (err error) {
	err = ioutil.WriteFile(b.blobPath(hash), data, 0644)	
	return
}

func (b *LocalBackend) blobPath(hash string) string {
	return filepath.Join(b.Directory, hash)
}

func NewLocalBackend(dir string) *LocalBackend {
	os.Mkdir(dir, 0744)
	return &LocalBackend{dir}
}

func (b *LocalBackend) Exists(hash string) bool {
	if _, err := os.Stat(b.blobPath(hash)); err == nil {
		return true
	}
	return false
}

func (b *LocalBackend) Get(hash string) (data []byte, err error) {
	data, err = ioutil.ReadFile(b.blobPath(hash))
	return
}

func (b *LocalBackend) Enumerate(blobs chan<- string) error {
	defer close(blobs)
	dirdata, err := ioutil.ReadDir(b.Directory)
	if err != nil {
		return err
	}
	for _, data := range dirdata {
		if !data.IsDir() {
			blobs <- data.Name()
		}
	}
	return nil
}
