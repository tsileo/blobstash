package reader // import "a4.io/blobstash/pkg/filetree/reader"

import (
	"fmt"
	"io"
	"os"

	"a4.io/blobstash/pkg/client/blobstore"
	"a4.io/blobstash/pkg/filetree/filetreeutil/meta"
	"a4.io/blobstash/pkg/filetree/reader/filereader"
)

type BlobStorer interface {
	// Get(context.Context, string) ([]byte, error)
	// Enumerate(chan<- string, string, string, int) error
	Stat(string) (bool, error)
	Put(string, []byte) error
}

type Downloader struct {
	bs *blobstore.BlobStore
}

func NewDownloader(bs *blobstore.BlobStore) *Downloader {
	return &Downloader{bs}
}

func (d *Downloader) Download(m *meta.Meta, path string) error {
	if _, err := os.Stat(path); err == nil {
		return fmt.Errorf("path already exists")
	}

	if m.IsFile() {
		if err := filereader.GetFile(d.bs, m.Hash, path); err != nil {
			return fmt.Errorf("failed to download file %s: %v", m.Hash, err)
		}
		return nil
	}

	if err := GetDir(d.bs, m.Hash, path); err != nil {
		return fmt.Errorf("failed to download directory %s: %v", m.Hash, err)
	}

	return nil
}

func (d *Downloader) File(m *meta.Meta) (io.ReadCloser, error) {
	return filereader.NewFile(d.bs, m), nil
}
