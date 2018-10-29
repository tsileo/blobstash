package filereader // import "a4.io/blobstash/pkg/filetree/reader/filereader"

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/hashicorp/golang-lru"
	"golang.org/x/crypto/blake2b"

	"a4.io/blobstash/pkg/filetree/filetreeutil/node"
)

// FIXME(tsileo): implements os.FileInfo

const (
	SEEK_SET int = 0 // seek relative to the origin of the file
	SEEK_CUR int = 1 // seek relative to the current offset
	SEEK_END int = 2 // seek relative to the end
)

type BlobStore interface {
	Get(context.Context, string) ([]byte, error)
}

// Download a file by its hash to path
func GetFile(ctx context.Context, bs BlobStore, hash, path string) error {
	// FIXME(tsileo): take a `*meta.Meta` as argument instead of the hash
	// readResult := &ReadResult{}
	buf, err := os.Create(path)
	defer buf.Close()
	if err != nil {
		return err
	}
	h, err := blake2b.New256(nil)
	if err != nil {
		panic(err)
	}
	js, err := bs.Get(ctx, hash)
	if err != nil {
		return err
	}
	meta, err := node.NewNodeFromBlob(hash, js)
	if err != nil {
		return fmt.Errorf("failed to get meta %v \"%s\": %v", hash, js, err)
	}
	meta.Hash = hash
	cache, err := lru.New(5)
	if err != nil {
		return err
	}
	ffile := NewFile(ctx, bs, meta, cache)
	defer ffile.Close()
	fileReader := io.TeeReader(ffile, h)
	io.Copy(buf, fileReader)
	// readResult.Hash = fmt.Sprintf("%x", h.Sum(nil))
	// readResult.FilesCount++
	// readResult.FilesDownloaded++
	fstat, err := buf.Stat()
	if err != nil {
		return err
	}
	if int(fstat.Size()) != meta.Size {
		return fmt.Errorf("file %s not successfully restored, size:%d/expected size:%d", path, fstat.Size(), meta.Size)
	}
	// TODO(tsileo): check against the full hash
	return nil
	// readResult.Size = int(fstat.Size())
	// readResult.SizeDownloaded = readResult.Size
	// if readResult.Size != meta.Size {
	// 	return readResult, fmt.Errorf("file %+v not successfully restored, size:%v/expected size:%v",
	// 		meta, readResult.Size, meta.Size)
	// }
	// return readResult, nil
}

// IndexValue represents a file chunk
type IndexValue struct {
	Index int64
	Value string
	I     int
}

// File implements io.Reader, and io.ReaderAt.
// It fetch blobs on the fly.
type File struct {
	name    string
	bs      BlobStore
	meta    *node.RawNode
	offset  int64
	size    int64
	llen    int
	lmrange []*IndexValue

	maxI int

	preloadOnce sync.Once

	lru *lru.Cache
	ctx context.Context
}

// NewFile creates a new File instance.
func NewFile(ctx context.Context, bs BlobStore, meta *node.RawNode, cache *lru.Cache) (f *File) {
	f = &File{
		bs:      bs,
		meta:    meta,
		size:    int64(meta.Size),
		lmrange: []*IndexValue{},
		lru:     cache,
		ctx:     ctx,
	}
	if fileRefs := meta.FileRefs(); fileRefs != nil {
		for idx, riv := range fileRefs {
			iv := &IndexValue{Index: riv.Index, Value: riv.Value, I: idx}
			f.lmrange = append(f.lmrange, iv)
		}
	}
	return
}

// NewFileRemote creates a new File instance that will fetch chunks from the remote storage, and uses BlobStash as a indexer only
func NewFileRemote(ctx context.Context, bs BlobStore, meta *node.RawNode, ivs []*node.IndexValue, cache *lru.Cache) (f *File) {
	f = &File{
		bs:      bs,
		meta:    meta,
		size:    int64(meta.Size),
		lmrange: []*IndexValue{},
		lru:     cache,
		ctx:     ctx,
	}
	if ivs != nil {
		for idx, riv := range ivs {
			iv := &IndexValue{Index: riv.Index, Value: "remote://" + riv.Value, I: idx}
			f.lmrange = append(f.lmrange, iv)
		}
	}
	return
}

// PreloadChunks all the chunks in a goroutine
func (f *File) PreloadChunks() {
	f.preloadOnce.Do(func() {
		go func() {
			if f.lru == nil {
				return
			}
			var lastPreloaded int
		L:
			for {
				time.Sleep(50 * time.Millisecond)
				// FIXME(tsileo): smarter preload, and support cancel via the ctx
				for _, iv := range f.lmrange[lastPreloaded:] {
					if iv.I > f.maxI+3 {
						// preload pause
						break
					}
					if iv.I == len(f.lmrange)-1 {
						// preload done
						break L
					}
					//bbuf, _, _ := f.client.Blobs.Get(iv.Value)
					if _, ok := f.lru.Get(iv.Value); !ok {
						bbuf, err := f.bs.Get(f.ctx, iv.Value)
						if err != nil {
							panic(fmt.Errorf("failed to fetch blob %v: %v", iv.Value, err))
						}
						f.lru.Add(iv.Value, bbuf)
					}
					lastPreloaded = iv.I
				}
			}
		}()
	})
}

// Close implements io.Closer
func (f *File) Close() error {
	return nil
}

// PurgeCache purges the in-mem chunk cache
func (f *File) PurgeCache() error {
	if f != nil {
		if f.lru != nil {
			f.lru.Purge()
		}
	}
	return nil
}

// ReadAt implements the io.ReaderAt interface
func (f *File) ReadAt(p []byte, offset int64) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}
	if f.size == 0 { // || f.offset >= f.size {
		return 0, io.EOF
	}
	buf, err := f.read(offset, len(p))
	if err != nil {
		return
	}
	n = copy(p, buf)
	return
}

// Low level read function, read a size from an offset
// Iterate only the needed blobs
func (f *File) read(offset int64, cnt int) ([]byte, error) {
	if cnt < 0 || int64(cnt) > f.size {
		cnt = int(f.size)
	}
	var buf bytes.Buffer
	var cbuf []byte
	var err error
	written := 0

	// if offset == f.size {
	// 	return nil, io.EOF
	// }

	if len(f.lmrange) == 0 {
		panic(fmt.Errorf("FakeFile %+v lmrange empty", f))
	}

	i := sort.Search(len(f.lmrange), func(i int) bool { return f.lmrange[i].Index >= offset })
	tiv := f.lmrange[i]

	for _, iv := range f.lmrange[tiv.I:] {
		if offset > iv.Index {
			continue
		}
		if iv.I > f.maxI {
			f.maxI = iv.I
		}
		if f.lru != nil {
			//bbuf, _, _ := f.client.Blobs.Get(iv.Value)
			if cached, ok := f.lru.Get(iv.Value); ok {
				cbuf = cached.([]byte)
			} else {
				bbuf, err := f.bs.Get(f.ctx, iv.Value)
				if err != nil {
					return nil, fmt.Errorf("failed to fetch blob %v: %v", iv.Value, err)
				}
				f.lru.Add(iv.Value, bbuf)
				cbuf = bbuf
			}
		} else {
			cbuf, err = f.bs.Get(f.ctx, iv.Value)
			if err != nil {
				return nil, fmt.Errorf("failed to fetch blob %v: %v", iv.Value, err)
			}
		}
		bbuf := cbuf
		foffset := 0
		if offset != 0 {
			// Compute the starting offset of the blob
			blobStart := iv.Index - int64(len(bbuf))
			// and subtract it to get the correct offset
			foffset = int(offset - int64(blobStart))
			offset = 0
		}
		// If the remaining cnt (cnt - written)
		// is greater than the blob slice
		if cnt-written > len(bbuf)-foffset {
			fwritten, err := buf.Write(bbuf[foffset:])
			if err != nil {
				return nil, err
			}
			written += fwritten

		} else {
			// What we need fit in this blob
			// it should return after this
			if foffset+cnt-written > len(bbuf) {
				panic(fmt.Errorf("failed to read from FakeFile %+v [%v:%v]", f, foffset, foffset+cnt-written))
			}
			fwritten, err := buf.Write(bbuf[foffset : foffset+cnt-written])
			if err != nil {
				return nil, err
			}

			written += fwritten
			// Check that the total written bytes equals the requested size
			if written != cnt {
				panic("error reading FakeFile")
			}
		}
		if written == cnt {
			return buf.Bytes(), nil
		}
		cbuf = nil
	}
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), err
}

// Reset resets the offset to 0
func (f *File) Reset() {
	f.offset = 0
}

// Read implements io.Reader
func (f *File) Read(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}
	if f.size == 0 || f.offset >= f.size {
		return 0, io.EOF
	}
	n = 0
	limit := len(p)
	if limit > int(f.size-f.offset) {
		limit = int(f.size - f.offset)
	}
	b, err := f.read(f.offset, limit)
	if err == io.EOF {
		return 0, io.EOF
	}
	if err != nil {
		return 0, fmt.Errorf("failed to read %+v at range %v-%v: %v", f, f.offset, limit, err)
	}
	n = copy(p, b)
	f.offset += int64(n)
	return
}

// Seek implements io.Seeker
func (f *File) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case SEEK_SET:
		f.offset = offset
	case SEEK_CUR:
		f.offset += offset
	case SEEK_END:
		f.offset = f.size - offset
	default:
		return 0, fmt.Errorf("invalid whence: %d", whence)
	}
	return f.offset, nil
}
