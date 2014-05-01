package models

import (
	"os"
	"crypto/sha1"
	"fmt"
	"github.com/tsileo/datadatabase/lru"
	"github.com/garyburd/redigo/redis"
	"io"
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
	fullHash := sha1.New()
	start := ""
	for {
		hs, err := redis.Strings(con.Do("LRANGE", key, start, "\xff", 50))
		if err != nil {
			return readResult, err
		}
		for _, hash := range hs {
			data, err := redis.String(con.Do("BGET", hash))
			if err != nil {
				panic(err)
			}
			bdata := []byte(data)
			if SHA1(bdata) != hash {
				panic("Corrupted")
			}
			fullHash.Write(bdata)
			buf.Write(bdata)
			buf.Sync()
			readResult.DownloadedCnt++
			readResult.DownloadedSize += len(bdata)
			readResult.Size += len(bdata)
			readResult.BlobsCnt++
		}
		if len(hs) < 50 {
			break
		} else {
			start = hs[49]
		}
	}
	readResult.Hash = fmt.Sprintf("%x", fullHash.Sum(nil))
	return readResult, nil
}


type FakeFile struct {
	pool *redis.Pool
	ref string
	offset int
	size int
	blobs *lru.LRU
}

func NewFakeFile(pool *redis.Pool, ref string) (f *FakeFile) {
	f = &FakeFile{pool:pool, ref:ref}
	f.blobs = lru.New(f.FetchBlob, 10)
	return
}

func (f *FakeFile) FetchBlob(hash interface{}) interface{} {
	con := f.pool.Get()
	defer con.Close()
	data, _ := redis.String(con.Do("BGET", hash.(string)))
	return data
}

func (f *FakeFile) Read(p []byte) (n int, err error) {
	if len(p) == 0 {
    	return 0, nil
    }
    if f.offset >= f.size {
    	return 0, io.EOF
    }
	n = 0
	n = copy(p, []byte("omg"))
	f.offset += n
	return
}
