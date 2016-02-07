package filereader

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"github.com/Workiva/go-datastructures/trie/yfast"
	"github.com/dchest/blake2b"
	"github.com/hashicorp/golang-lru"

	"github.com/tsileo/blobstash/ext/filetree/filetreeutil/meta"
)

type BlobStore interface {
	Get(hash string) ([]byte, error)
}

// Download a file by its hash to path
func GetFile(bs BlobStore, hash, path string) error {
	// readResult := &ReadResult{}
	buf, err := os.Create(path)
	defer buf.Close()
	if err != nil {
		return err
	}
	h := blake2b.New256()
	js, err := bs.Get(hash)
	if err != nil {
		return err
	}
	meta, err := meta.NewMetaFromBlob(hash, js)
	if err != nil {
		return fmt.Errorf("failed to get meta %v: %v", hash, err)
	}
	meta.Hash = hash
	ffile := NewFile(bs, meta)
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
		return fmt.Errorf("file %+v not successfully restored, size:%d/expected size:%d", fstat.Size, meta.Size)
	}
	return nil
	// readResult.Size = int(fstat.Size())
	// readResult.SizeDownloaded = readResult.Size
	// if readResult.Size != meta.Size {
	// 	return readResult, fmt.Errorf("file %+v not successfully restored, size:%v/expected size:%v",
	// 		meta, readResult.Size, meta.Size)
	// }
	// return readResult, nil
}

type IndexValue struct {
	Index int
	Value string
	I     int
}

// Key is needed for yfast
func (iv *IndexValue) Key() uint64 {
	return uint64(iv.Index)
}

// FakeFile implements io.Reader, and io.ReaderAt.
// It fetch blobs on the fly.
type File struct {
	name    string
	bs      BlobStore
	meta    *meta.Meta
	offset  int
	size    int
	llen    int
	lmrange []*IndexValue
	trie    *yfast.YFastTrie
	lru     *lru.Cache
}

// NewFakeFile creates a new FakeFile instance.
func NewFile(bs BlobStore, meta *meta.Meta) (f *File) {
	// Needed for the blob routing
	cache, err := lru.New(2)
	if err != nil {
		panic(err)
	}
	f = &File{
		bs:      bs,
		meta:    meta,
		size:    meta.Size,
		lmrange: []*IndexValue{},
		trie:    yfast.New(uint64(0)),
		lru:     cache,
	}
	if meta.Size > 0 {
		for idx, m := range meta.Refs {
			data := m.([]interface{})
			var index int
			switch i := data[0].(type) {
			case float64:
				index = int(i)
			case int:
				index = i
			default:
				panic("unexpected index")
			}
			iv := &IndexValue{Index: index, Value: data[1].(string), I: idx}
			f.lmrange = append(f.lmrange, iv)
			f.trie.Insert(iv)
		}
	}
	return
}

func (f *File) Close() error {
	f.lru.Purge()
	return nil
}

// ReadAt implements the io.ReaderAt interface
func (f *File) ReadAt(p []byte, offset int64) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}
	if f.size == 0 || f.offset >= f.size {
		return 0, io.EOF
	}
	buf, err := f.read(int(offset), len(p))
	if err != nil {
		return
	}
	n = copy(p, buf)
	return
}

// Low level read function, read a size from an offset
// Iterate only the needed blobs
func (f *File) read(offset, cnt int) ([]byte, error) {
	//log.Printf("FakeFile %v read(%v, %v)", f.ref, offset, cnt)
	if cnt < 0 || cnt > f.size {
		cnt = f.size
	}
	var buf bytes.Buffer
	var cbuf []byte
	var err error
	written := 0

	if len(f.lmrange) == 0 {
		panic(fmt.Errorf("FakeFile %+v lmrange empty", f))
	}

	tiv := f.trie.Successor(uint64(offset)).(*IndexValue)
	if tiv.Index == offset {
		tiv = f.trie.Successor(uint64(offset + 1)).(*IndexValue)
	}
	for _, iv := range f.lmrange[tiv.I:] {
		if offset > iv.Index {
			continue
		}
		//bbuf, _, _ := f.client.Blobs.Get(iv.Value)
		if cached, ok := f.lru.Get(iv.Value); ok {
			cbuf = cached.([]byte)
		} else {
			bbuf, err := f.bs.Get(iv.Value)
			if err != nil {
				return nil, fmt.Errorf("failed to fetch blob %v: %v", iv.Value, err)
			}
			f.lru.Add(iv.Value, bbuf)
			cbuf = bbuf
		}
		bbuf := cbuf
		foffset := 0
		if offset != 0 {
			// Compute the starting offset of the blob
			blobStart := iv.Index - len(bbuf)
			// and subtract it to get the correct offset
			foffset = offset - blobStart
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
	if limit > (f.size - f.offset) {
		limit = f.size - f.offset
	}
	b, err := f.read(f.offset, limit)
	if err == io.EOF {
		return 0, io.EOF
	}
	if err != nil {
		return 0, fmt.Errorf("failed to read %+v at range %v-%v", f, f.offset, limit)
	}
	n = copy(p, b)
	f.offset += n
	return
}
