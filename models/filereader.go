package models

import (
	"os"
	"crypto/sha1"
	"fmt"
	"github.com/garyburd/redigo/redis"
)

func (client *Client) GetFile(key, path string) (*ReadResult, error) {
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
