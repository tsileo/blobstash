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

func Do(verb, url string, c *s3util.Config) (int, error) {
	if c == nil {
		c = s3util.DefaultConfig
	}
	// TODO(kr): maybe parallel range fetching
	r, _ := http.NewRequest(verb, url, nil)
	r.Header.Set("Date", time.Now().UTC().Format(http.TimeFormat))
	c.Sign(r, *c.Keys)
	client := c.Client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(r)
	if err != nil {
		return resp.StatusCode, err
	}
	if resp.StatusCode != 200 && resp.StatusCode != 204 && resp.StatusCode != 404 {
		var buf bytes.Buffer
		io.Copy(&buf, resp.Body)
		resp.Body.Close()
		return resp.StatusCode, fmt.Errorf("Bad response code for query %v/%v, %v: %+v", verb, url, resp.StatusCode, buf.String())
	}
	return resp.StatusCode, nil
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
	backend := &S3Backend{Bucket: bucket, keys: &keys}
	if err := backend.load(); err != nil {
		panic(fmt.Errorf("Error loading %T: %v", backend, err))
	}
	return backend
}

func (backend *S3Backend) bucket(key string) string {
	return fmt.Sprintf("https://%v.s3.amazonaws.com/%v", backend.Bucket, key)
}

func (backend *S3Backend) Close() {
	return
}

func (backend *S3Backend) load() error {
	log.Println("S3Backend: checking if backend exists")
	exists, err := Do("HEAD", backend.bucket(""), nil)
	if err != nil {
		return err
	}
	if exists != 200 {
		log.Printf("S3Backend: creating bucket %v", backend.Bucket)
		created, err := Do("PUT", backend.bucket(""), nil)
		if created != 200 || err != nil {
			log.Println("S3Backend: error creating bucket: %v", err)
			return err
		}
	}
	return nil
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
	r, err := Do("HEAD", backend.bucket(hash), nil)
	if r == 200 {
		return true
	} else if r == 404 {
		return false
	} else {
		log.Printf("S3Backend: error performing HEAD request for blob %v, err:%v", hash, err)
	}
	return false
}

func (backend *S3Backend) Delete(hash string) (error) {
	_, err := Do("DELETE", backend.bucket(hash), nil)
	return err
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

// Delete all keys in a bucket (assumes the directory is flat/no sub-directories).
func (backend *S3Backend) Drop() error {
	log.Printf("S3Backend: dropping bucket...")
	blobs := make(chan string)
	go backend.Enumerate(blobs)
	for blob := range blobs {
		if err := backend.Delete(blob); err != nil {
			return err
		}
	}
	return nil
}
