package client

import (
	"bytes"
	"crypto/sha1"
	"errors"
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
		panic("Error FetchBlob")
	}
	buf.WriteString(data)
	return buf.Bytes()
}

// Download a file by its hash to path
func (client *Client) GetFile(key, path string) (*ReadResult, error) {
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
	ffile := NewFakeFile(client, meta.Ref, meta.Size)
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
		return readResult, fmt.Errorf("File %+v not successfully restored, size:%v/expected size:%v",
			meta, readResult.Size, meta.Size)
	}
	return readResult, nil
}

// FakeFile implements io.Reader, and io.ReaderAt.
// It fetch blobs on the fly.
type FakeFile struct {
	client *Client
	ref    string
	offset int
	size   int
}

// Create a new FakeFile instance.
func NewFakeFile(client *Client, ref string, size int) (f *FakeFile) {
	f = &FakeFile{client: client, ref: ref, size: size}
	return
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
	written := 0
	var indexValueList []struct {
		Index int
		Value string
	}
	con := f.client.Pool.Get()
	defer con.Close()

	// Workaround:
	// Sometimes, a LMRANGE returns EOF when seeking for no reason,
	// we adjust the offset when nil is returned until it works.
	ok := false
	tryCnt := 0
	tcnt := 1.0
	var values []interface{}
	var err error
	for !ok {
		if tryCnt > 5 {
			break
		}
		values, err = redis.Values(con.Do("LMRANGE", f.ref, int(float64(offset)*tcnt), offset+cnt, 0))
		if err == nil {
			break
		}
		tcnt = tcnt*0.9
		tryCnt++
	}
	redis.ScanSlice(values, &indexValueList)
	for _, iv := range indexValueList {
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
			//log.Printf("(1) (cnt:%v/written:%v/foffset:%v/buf len:%v/fwritten:%v)", cnt, written, foffset, len(bbuf), fwritten)
			if err != nil {
				return nil, err
			}
			written += fwritten

		} else {
			// What we need fit in this blob
			// it should return after this
			fwritten, err := buf.Write(bbuf[foffset : foffset+cnt-written])
			//log.Printf("(2) %v-%v (cnt:%v/written:%v/foffset:%v/buf len:%v/fwritten:%v)", foffset, foffset+cnt-written, cnt, written, foffset, len(bbuf), fwritten)
			if err != nil {
				return nil, err
			}

			written += fwritten
			// Check that the total written bytes equals the requested size
			if written != cnt {
				panic("Error reading FakeFile")
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
		return 0, errors.New("datadb: Error reading slice from blobs")
	}
	n = copy(p, b)
	f.offset += n
	return
}
