package client

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"io"
	"os"

	"github.com/garyburd/redigo/redis"
)

type BlobFetcher interface {
	Get(string) ([]byte, bool, error)
	Close()
	Remove()
}

// FetchBlob is used by the client level blobs LRU
// Return the data for the given hash
func (client *Client) FetchBlob(hash string) []byte {
	con := client.Pool.Get()
	defer con.Close()
	var buf bytes.Buffer
	data, err := redis.String(con.Do("BGET", hash))
	if err != nil {
		panic(fmt.Errorf("failed to fetch blob %v", hash))
	}
	buf.WriteString(data)
	return buf.Bytes()
}

// Download a file by its hash to path
func (client *Client) GetFile(host, key, path string) (*ReadResult, error) {
	// TODO(ts) make io.Copy ?
	readResult := &ReadResult{}
	con := client.Pool.Get()
	defer con.Close()
	buf, err := os.Create(path)
	defer buf.Close()
	if err != nil {
		return nil, err
	}
	h := sha1.New()
	meta, err := NewMetaFromDB(client.Pool, key)
	if err != nil {
		return nil, err
	}
	ffile := NewFakeFile(client, host, meta.Ref, meta.Size)
	defer ffile.Close()
	ffilreReader := io.TeeReader(ffile, h)
	io.Copy(buf, ffilreReader)
	readResult.Hash = fmt.Sprintf("%x", h.Sum(nil))
	readResult.FilesCount++
	readResult.FilesDownloaded++
	fstat, err := buf.Stat()
	if err != nil {
		return readResult, err
	}
	readResult.Size = int(fstat.Size())
	readResult.SizeDownloaded = readResult.Size
	if readResult.Size != meta.Size {
		return readResult, fmt.Errorf("file %+v not successfully restored, size:%v/expected size:%v",
			meta, readResult.Size, meta.Size)
	}
	return readResult, nil
}

// FakeFile implements io.Reader, and io.ReaderAt.
// It fetch blobs on the fly.
type FakeFile struct {
	client *Client
	oldHost string
	ref    string
	offset int
	size   int
	lmrange []struct {
		Index int
		Value string
	}
}


// Create a new FakeFile instance.
func NewFakeFile(client *Client, host, ref string, size int) (f *FakeFile) {
	// Needed for the blob routing
	oldHost := client.Hostname
	client.Hostname = host
	f = &FakeFile{client: client, oldHost: oldHost, ref: ref, size: size}
	con := f.client.Pool.Get()
	defer con.Close()
	values, err := redis.Values(con.Do("LITER", f.ref, "WITH", "RANGE"))
	if err != nil {
		panic(fmt.Errorf("error [LITER %v WITH RANGE]: %v", f.ref, err))
	}
	redis.ScanSlice(values, &f.lmrange)
	return
}

func (f *FakeFile) Close() error {
	f.client.Hostname = f.oldHost
	return nil
}

// Implement the io.ReaderAt interface
func (f *FakeFile) ReadAt(p []byte, offset int64) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}
	if f.offset >= f.size {
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
func (f *FakeFile) read(offset, cnt int) ([]byte, error) {
	//log.Printf("FakeFile %v read(%v, %v)", f.ref, offset, cnt)
	if cnt < 0 || cnt > f.size {
		cnt = f.size
	}
	var buf bytes.Buffer
	var err error
	written := 0

	if len(f.lmrange) == 0 {
		panic(fmt.Errorf("FakeFile %+v lmrange empty", f))
	}

	for _, iv := range f.lmrange {
		if offset > iv.Index {
			continue
		}
		bbuf, _, _ := f.client.Blobs.Get(iv.Value)
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
	}
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), err
}

// Reset the offset to 0
func (f *FakeFile) Reset() {
	f.offset = 0
}

// Read implement io.Reader
func (f *FakeFile) Read(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}
	if f.offset >= f.size {
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
