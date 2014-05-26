/*

Package s3 implement the AWS S3 backend for storing blobs.

The bucket must already exists.

*/
package s3

import (
	"os"
	"bytes"
	"fmt"
	"io"
	"bufio"
	"log"
	"time"
	"net/http"

	ks3 "github.com/kr/s3"
	"github.com/kr/s3/s3util"
	"sync"
)

func s3Exists(url string, c *s3util.Config) (bool, error) {
	if c == nil {
		c = s3util.DefaultConfig
	}
	// TODO(kr): maybe parallel range fetching
	r, _ := http.NewRequest("HEAD", url, nil)
	r.Header.Set("Date", time.Now().UTC().Format(http.TimeFormat))
	c.Sign(r, *c.Keys)
	client := c.Client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(r)
	if err != nil {
		return false, err
	}
	if resp.StatusCode != 200 {
		return false, fmt.Errorf("Bad response: %+v", resp)
	}
	return true, nil
}

type S3Backend struct {
	Bucket string
	keys *ks3.Keys
	sync.Mutex
}

func New(bucket string) *S3Backend {
	log.Println("S3Backend: starting")
	keys := ks3.Keys{
		AccessKey: os.Getenv("S3_ACCESS_KEY"),
		SecretKey: os.Getenv("S3_SECRET_KEY"),
	}

	s3util.DefaultConfig.AccessKey = keys.AccessKey
	s3util.DefaultConfig.SecretKey = keys.SecretKey
	return &S3Backend{Bucket: bucket, keys: &keys}
}

func (backend *S3Backend) bucket(key string) string {
	return fmt.Sprintf("https://%v.s3.amazonaws.com/%v", backend.Bucket, key)
}

func (backend *S3Backend) Close() {
	return
}

func (backend *S3Backend) Put(hash string, data []byte) error {
	backend.Lock()
	defer backend.Unlock()
	r := bytes.NewBuffer(data)
	w, err := s3util.Create(fmt.Sprintf("%v%v", backend.bucket(""), hash), nil, nil)
	if err != nil {
		return err
	}
	_, err = io.Copy(w, r)
	if err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	return nil
}

func (backend *S3Backend) Get(hash string) ([]byte, error) {
	r, err := s3util.Open(backend.bucket(hash), nil)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	var buf bytes.Buffer
	w := bufio.NewWriter(&buf)
	io.Copy(w, r)
	return buf.Bytes(), nil
}

func (backend *S3Backend) Exists(hash string) (bool) {
	// TODO(tsileo) HEAD request!
	r, _ := s3Exists(backend.bucket(hash), nil)
	return r
}

func (backend *S3Backend) Enumerate(blobs chan<- string) error {
	defer close(blobs)
	f, err := s3util.NewFile(backend.bucket(""), nil)
	if err != nil {
		return err
	}
	var infos []os.FileInfo
	for {
		infos, err = f.Readdir(0)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		for _, info := range infos {
			c := info.Sys().(*s3util.Stat)
			blobs <- c.Key
		}
	}
	return nil
}
