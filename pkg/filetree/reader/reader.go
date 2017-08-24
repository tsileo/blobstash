package reader // import "a4.io/blobstash/pkg/filetree/reader"

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/hashicorp/golang-lru"

	"a4.io/blobstash/pkg/client/blobstore"
	"a4.io/blobstash/pkg/filetree/filetreeutil/node"
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

func (d *Downloader) Download(ctx context.Context, m *node.RawNode, path string) error {
	if _, err := os.Stat(path); err == nil {
		return fmt.Errorf("path already exists")
	}

	if m.IsFile() {
		if err := filereader.GetFile(ctx, d.bs, m.Hash, path); err != nil {
			return fmt.Errorf("failed to download file %s: %v", m.Hash, err)
		}
		return nil
	}

	if err := GetDir(ctx, d.bs, m.Hash, path); err != nil {
		return fmt.Errorf("failed to download directory %s: %v", m.Hash, err)
	}

	return nil
}

func (d *Downloader) File(ctx context.Context, m *node.RawNode) (io.ReadCloser, error) {
	cache, err := lru.New(2)
	if err != nil {
		return nil, err
	}
	return filereader.NewFile(ctx, d.bs, m, cache), nil
}
