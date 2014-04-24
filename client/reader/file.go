package reader

import (
	"os"
	"crypto/sha1"
	"fmt"
	"time"
	"github.com/garyburd/redigo/redis"
)

func GetDbPool() (pool *redis.Pool, err error) {
	pool = &redis.Pool{
		MaxIdle:     50,
		MaxActive: 50,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", "localhost:9736")
			if err != nil {
				return nil, err
			}
			//if _, err := c.Do("AUTH", password); err != nil {
			//    c.Close()
			//    return nil, err
			//}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
	return
}

func SHA1(data []byte) string {
	h := sha1.New()
	h.Write(data)
	return fmt.Sprintf("%x", h.Sum(nil))
}

func FileReader(key, path string) (*ReadResult, error) {
	readResult := &ReadResult{}
	rpool, _ := GetDbPool()
	con := rpool.Get()
	defer con.Close()
	buf, err := os.Create(path)
	defer buf.Close()
	if err != nil {
		return readResult, err
	}
	fullHash := sha1.New()
	start := ""
	snapId, _ := redis.String(con.Do("SNAPSHOT"))
	defer func(rpool *redis.Pool, snapId string) {
		rcon := rpool.Get()
		defer rcon.Close()
		rcon.Do("SNAPRELEASE", snapId)
	}(rpool, snapId)
	for {
		hs, _ := redis.Strings(con.Do("BPRANGE", snapId, key, start, "\xff", 50))
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
