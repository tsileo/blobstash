/*

Package s3 implement the AWS S3 backend for storing blobs.

The bucket must already exists.

*/
package s3

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/fatih/structs"
	ks3 "github.com/kr/s3"
	"github.com/kr/s3/s3util"
	"github.com/tsileo/blobstash/logger"
	log2 "gopkg.in/inconshreveable/log15.v2"
)

type Config struct {
	Bucket       string `structs:"bucket,omitempty"`
	Location     string `structs:"location,omitempty"`
	AwsAccessKey string `structs:"aws_access_key,omitempty"`
	AwsSecretKey string `structs:"aws_secret_key,omitempty"`
}

func (c *Config) Backend() string {
	return "s3"
}

func (c *Config) Config() map[string]interface{} {
	return map[string]interface{}{
		"backend-type": c.Backend(),
		"backend-args": c.Map(),
	}
}

func (c *Config) Map() map[string]interface{} {
	if c.AwsAccessKey != "" {
		s3util.DefaultConfig.AccessKey = c.AwsAccessKey
		s3util.DefaultConfig.SecretKey = c.AwsSecretKey
	}
	return structs.Map(c)
}

type S3Backend struct {
	log       log2.Logger
	Bucket    string
	Location  string
	BucketURL string
	keys      *ks3.Keys
	sync.Mutex
}

func New(bucket, location string) *S3Backend {
	keys := ks3.Keys{
		AccessKey: os.Getenv("S3_ACCESS_KEY"),
		SecretKey: os.Getenv("S3_SECRET_KEY"),
	}

	if s3util.DefaultConfig.AccessKey == "" && (keys.AccessKey == "" || keys.SecretKey == "") {
		panic("S3_ACCESS_KEY or S3_SECRET_KEY not set")
	}

	if s3util.DefaultConfig.AccessKey != "" {
		keys.AccessKey = s3util.DefaultConfig.AccessKey
		keys.SecretKey = s3util.DefaultConfig.SecretKey
	} else {
		s3util.DefaultConfig.AccessKey = keys.AccessKey
		s3util.DefaultConfig.SecretKey = keys.SecretKey
	}
	backend := &S3Backend{Bucket: bucket, Location: location, keys: &keys}
	backend.log = logger.Log.New("backend", backend.String())
	backend.log.Debug("Started", "bucket", bucket, "location", location)
	return backend
}

func (backend *S3Backend) String() string {
	return fmt.Sprintf("s3-%v", backend.Bucket)
}

func (backend *S3Backend) bucket(dir string) string {
	// dir must end with a slash
	return fmt.Sprintf("https://%v.s3-%v.amazonaws.com/%v", backend.Bucket, backend.Location, dir)
}

func (backend *S3Backend) key(key string) string {
	if key != "" {
		key = key[0:2] + "/" + key
	}
	return backend.bucket("") + key
}

func (backend *S3Backend) Close() {
	return
}

func (backend *S3Backend) Done() error {
	return nil
}

func (backend *S3Backend) upload(hash string, data []byte) error {
	backend.Lock()
	defer backend.Unlock()
	r := bytes.NewBuffer(data)
	w, err := s3util.Create(backend.key(hash), nil, nil)
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

func (backend *S3Backend) Put(hash string, data []byte) error {
	var err error
	for tries, retry := 0, true; tries < 2 && retry; tries++ {
		retry = false
		err = backend.upload(hash, data)
		if err != nil {
			retry = true
			time.Sleep(1 * time.Second)
		}
	}
	return err
}

func (backend *S3Backend) Get(hash string) ([]byte, error) {
	r, err := s3util.Open(backend.key(hash), nil)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	var buf bytes.Buffer
	w := bufio.NewWriter(&buf)
	io.Copy(w, r)
	return buf.Bytes(), nil
}

func (backend *S3Backend) PlainText(hash string) ([]byte, error) {
	r, err := http.NewRequest("GET", backend.key(hash), nil)
	r.Header.Set("Date", time.Now().UTC().Format(http.TimeFormat))
	r.Header.Set("Range", fmt.Sprintf("bytes=0-85"))
	ks3.Sign(r, *backend.keys)
	resp, err := http.DefaultClient.Do(r)
	defer resp.Body.Close()
	res, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("S3Backend: error performing GET range request for blob %v, err:%v", hash, err)
	}
	if resp.StatusCode == 206 {
		return res, nil
	}
	return nil, fmt.Errorf("req failed %v: %s", resp.StatusCode, res)
}

func (backend *S3Backend) Exists(hash string) (bool, error) {
	r, err := http.NewRequest("HEAD", backend.key(hash), nil)
	r.Header.Set("Date", time.Now().UTC().Format(http.TimeFormat))
	ks3.Sign(r, *backend.keys)
	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		return false, fmt.Errorf("S3Backend: error performing HEAD request for blob %v, err:%v", hash, err)
	}
	if resp.StatusCode == 200 {
		return true, nil
	}
	return false, nil
}

func (backend *S3Backend) Delete(hash string) error {
	r, err := http.NewRequest("DELETE", backend.key(hash), nil)
	if err != nil {
		return err
	}
	r.Header.Set("Date", time.Now().UTC().Format(http.TimeFormat))
	ks3.Sign(r, *backend.keys)
	_, err = http.DefaultClient.Do(r)
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
			sf, err := s3util.NewFile(backend.bucket(info.Name()+"/"), nil)
			if err != nil {
				return err
			}
			var subinfos []os.FileInfo
			for {
				subinfos, err = sf.Readdir(0)
				if err == io.EOF {
					break
				} else if err != nil {
					return err
				}
				for _, subinfo := range subinfos {
					blobs <- subinfo.Name()[3:] //subinfo.Sys().(*s3util.Stat).Key
				}
			}
		}
	}
	return nil
}

// Delete all keys in a bucket (assumes the directory is flat/no sub-directories).
func (backend *S3Backend) Drop() error {
	backend.log.Debug("dropping bucket...")
	blobs := make(chan string)
	go backend.Enumerate(blobs)
	for blob := range blobs {
		if err := backend.Delete(blob); err != nil {
			return err
		}
	}
	return backend.Delete("")
}
