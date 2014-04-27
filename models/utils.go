package models

import (
	"os"
	"crypto/sha1"
	"time"
	"bufio"
	"fmt"
	"io"
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

func FullSHA1(path string) string {
	f, _ := os.Open(path)
	defer f.Close()
	reader := bufio.NewReader(f)
	h := sha1.New()
	_, _ = io.Copy(h, reader)
	return fmt.Sprintf("%x", h.Sum(nil))
}

type WriteResult struct {
	Filename string
	Hash string
	Size int
	BlobsCnt int
	SkippedCnt int
	SkippedSize int
	UploadedCnt int
	UploadedSize int
}

type ReadResult struct {
	Hash string
	Size int
	BlobsCnt int
//	SkippedCnt int
//	SkippedSize int
	DownloadedCnt int
	DownloadedSize int
}
