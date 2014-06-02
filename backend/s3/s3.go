/*

Package s3 implement the AWS S3 backend for storing blobs.

The bucket must already exists.

*/
package s3

import (
	"os"
	"bytes"
	"fmt"
	"encoding/xml"
	"io"
	"bufio"
	"log"
	"time"
	"strings"
	"net/http"

	ks3 "github.com/kr/s3"
	"github.com/kr/s3/s3util"
	"sync"
)

func do(verb, url string, body io.Reader, c *s3util.Config) (int, error) {
	if c == nil {
		c = s3util.DefaultConfig
	}
	// TODO(kr): maybe parallel range fetching
	r, err := http.NewRequest(verb, url, body)
	//r.ContentLength = 
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
	if resp.StatusCode != 200 && resp.StatusCode != 204 && resp.StatusCode != 404 && resp.StatusCode != 403 {
		var buf bytes.Buffer
		io.Copy(&buf, resp.Body)
		resp.Body.Close()
		return resp.StatusCode, fmt.Errorf("Bad response code for query %v/%v, %v: %+v",
			verb, url, resp.StatusCode, buf.String())
	}
	return resp.StatusCode, nil
}

type S3Backend struct {
	Bucket string
	Location string
	BucketURL string
	keys *ks3.Keys
	sync.Mutex
}

func New(bucket, location string) *S3Backend {
	log.Printf("S3Backend: starting")
	log.Printf("s3Backend: bucket:%v/location:%v", bucket, location)
	keys := ks3.Keys{
		AccessKey: os.Getenv("S3_ACCESS_KEY"),
		SecretKey: os.Getenv("S3_SECRET_KEY"),
	}

	if keys.AccessKey == "" || keys.SecretKey == "" {
		panic("S3_ACCESS_KEY or S3_SECRET_KEY not set")
	}

	s3util.DefaultConfig.AccessKey = keys.AccessKey
	s3util.DefaultConfig.SecretKey = keys.SecretKey
	backend := &S3Backend{Bucket: bucket, Location: location, keys: &keys}
	if err := backend.load(); err != nil {
		panic(fmt.Errorf("Error loading %T: %v", backend, err))
	}
	log.Printf("S3Backend: backend id => %v", backend.String())
	return backend
}

func (backend *S3Backend) String() string {
	return fmt.Sprintf("s3-%v", backend.Bucket)
}

func (backend *S3Backend) bucket(key string) string {
	//"https://%v.s3.amazonaws.com/%v"
	return fmt.Sprintf("https://%v.%v/%v", backend.Bucket, backend.BucketURL, key)
}

func (backend *S3Backend) Close() {
	return
}

type CreateBucketConfiguration struct {
	XMLName	xml.Name	`xml:"CreateBucketConfiguration"`
	Xmlns	string	`xml:"xmlns,attr"`
	LocationConstraint	string	`xml:"LocationConstraint"`
}

type LocationConstraint struct {
	XMLName  xml.Name `xml:"LocationConstraint"`
	Location string   `xml:",chardata"`
}

const s3Xmlns = "http://s3.amazonaws.com/doc/2006-03-01/"

func newCreateBucketConfiguration(location string) io.Reader {
	if location == "" {
		return nil
	}
	conf := &CreateBucketConfiguration{Xmlns: s3Xmlns, LocationConstraint: location}
	out, err := xml.Marshal(conf)
	if err != nil {
		panic(err)
	}
	return strings.NewReader(string(out))
}

func (backend *S3Backend) bucketLocation(bucket string) (string, error) {
	c := s3util.DefaultConfig
	r, _ := http.NewRequest("GET", fmt.Sprintf("https://%v.s3.amazonaws.com?location", backend.Bucket), nil)
	r.Header.Set("Date", time.Now().UTC().Format(http.TimeFormat))
	c.Sign(r, *c.Keys)
	client := c.Client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(r)
	if err != nil {
		return "", err
	}
	var locationConstraint LocationConstraint
	if err := xml.NewDecoder(resp.Body).Decode(&locationConstraint); err != nil {
		return "", err
	}
	if locationConstraint.Location == "" {
		return "s3.amazonaws.com", nil
	}
	return fmt.Sprintf("s3-%v.amazonaws.com", locationConstraint.Location), nil
}

// load check if the bucket already exists, if not, will try to create it.
// It also fetch the bucket location.
func (backend *S3Backend) load() error {
	log.Println("S3Backend: checking if backend exists")
	exists, err := do("HEAD", "https://" + backend.Bucket + ".s3.amazonaws.com", nil, nil)
	if err != nil {
		return err
	}
	if exists == 404 {
		log.Printf("S3Backend: creating bucket %v", backend.Bucket)
		created, err := do("PUT", "https://" + backend.Bucket + ".s3.amazonaws.com", newCreateBucketConfiguration(backend.Location), nil)
		if created != 200 || err != nil {
			log.Println("S3Backend: error creating bucket: %v", err)
			return err
		}
	}
	burl, err := backend.bucketLocation(backend.Bucket)
	if err != nil {
		return fmt.Errorf("can't find bucket location: %v", err)
	}
	backend.BucketURL = burl
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
	r, err := do("HEAD", backend.bucket(hash), nil, nil)
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
	_, err := do("DELETE", backend.bucket(hash), nil, nil)
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
	_, err := do("DELETE", backend.bucket(""), nil, nil)
	return err
}
