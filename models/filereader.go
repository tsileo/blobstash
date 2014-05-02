package models

import (
	"os"
	"github.com/tsileo/datadatabase/lru"
	"github.com/garyburd/redigo/redis"
	"io"
	_ "log"
	"bytes"
	"errors"
)

func (client *Client) GetFile(key, path string) (*ReadResult, error) {
	// TODO(ts) make io.Copy ?
	readResult := &ReadResult{}
	con := client.Pool.Get()
	defer con.Close()
	buf, err := os.Create(path)
	defer buf.Close()
	if err != nil {
		return readResult, err
	}
	meta, _ := NewMetaFromDB(client.Pool, key) 
	ffile := NewFakeFile(client.Pool, meta.Hash, meta.Size)
	io.Copy(buf, ffile)
	readResult.Hash = meta.Hash
	return readResult, nil
}


type FakeFile struct {
	pool *redis.Pool
	ref string
	offset int
	size int
	blobs *lru.LRU
}

func NewFakeFile(pool *redis.Pool, ref string, size int) (f *FakeFile) {
	f = &FakeFile{pool:pool, ref:ref, size: size}
	f.blobs = lru.New(f.FetchBlob, 10)
	return
}

func (f *FakeFile) FetchBlob(hash interface{}) interface{} {
	con := f.pool.Get()
	defer con.Close()
	var buf bytes.Buffer
	data, err := redis.String(con.Do("BGET", hash.(string)))
	if err != nil {
		panic("Error FetchBlob")
	}
	buf.WriteString(data)
	return buf.Bytes()
}


func (f* FakeFile) ReadAt(p []byte, offset int64) (n int, err error) {
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

func (f *FakeFile) read(offset, cnt int) ([]byte, error) {
	if cnt < 0 || cnt > f.size {
		cnt = f.size
	}
	var buf bytes.Buffer
	written := 0
	var indexValueList []struct {
	    Index int
	    Value string
	}
	con := f.pool.Get()
	defer con.Close()
	values, err := redis.Values(con.Do("LMRANGE", f.ref, offset, offset+cnt, 0))
	if err != nil {
		return nil, err
	}
	redis.ScanSlice(values, &indexValueList)
	for _, iv := range indexValueList {
		bbuf := f.blobs.Get(iv.Value).([]byte)
		foffset := 0
		if offset != 0 {
			blobStart := iv.Index - len(bbuf)
			foffset =  offset - blobStart
			offset = 0
		}
		if cnt - written > len(bbuf) - foffset {
			fwritten, err := buf.Write(bbuf[foffset:])
			if err != nil {
				return nil, err
			}
			written += fwritten
			
		} else {
			fwritten, err := buf.Write(bbuf[foffset:foffset + cnt - written])
			if err != nil {
				return nil, err
			}
			written += fwritten
			if written != cnt {
				panic("Error reading FakeFile")
			}
		}

		if foffset == len(bbuf) {
			return nil, io.EOF
		}
		if written == cnt {
			return buf.Bytes(), nil
		}
	}
	return nil, err
}

// Reset the offset to 0
func (f *FakeFile) Reset() {
	f.offset = 0
}

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
