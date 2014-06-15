package client

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/tsileo/datadatabase/disklru"
	"github.com/tsileo/datadatabase/lru"
	"os"
	"path/filepath"
	"time"
)

var (
	uploaders    = 25 // concurrent upload uploaders
	dirUploaders = 25 // concurrent directory uploaders
)

func GetDbPool() (pool *redis.Pool, err error) {
	pool = &redis.Pool{
		MaxIdle:     250,
		MaxActive:   250,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", "localhost:9736")
			if err != nil {
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
	return
}

type Client struct {
	Pool         *redis.Pool
	Blobs        BlobFetcher
	Dirs         DirFetcher
	Metas        MetaFetcher
	uploaders    chan struct{}
	dirUploaders chan struct{}
	ignoredFiles []string
}

func NewClient(ignoredFiles []string) (*Client, error) {
	pool, err := GetDbPool()
	if err != nil {
		return nil, err
	}
	c := &Client{Pool: pool, uploaders: make(chan struct{}, uploaders),
		dirUploaders: make(chan struct{}, dirUploaders)}
	c.Blobs, err = disklru.New("./tmp_blobs_lru", c.FetchBlob, 536870912)
	c.Dirs = lru.New(c.FetchDir, 512)
	c.Metas = lru.New(c.FetchMeta, 512)
	for _, ignoredFile := range ignoredFiles {
		_, err := filepath.Match(ignoredFile, "check")
		if err != nil {
			return nil, fmt.Errorf("bad ignoredFiles pattern: %v", ignoredFile)
		}
	}
	c.ignoredFiles = ignoredFiles
	return c, err
}

func NewTestClient() (*Client, error) {
	pool, err := GetDbPool()
	if err != nil {
		return nil, err
	}
	c := &Client{Pool: pool, uploaders: make(chan struct{}, uploaders),
		dirUploaders: make(chan struct{}, dirUploaders)}
	c.Blobs, err = disklru.NewTest(c.FetchBlob, 536870912)
	c.Dirs = lru.New(c.FetchDir, 512)
	c.Metas = lru.New(c.FetchMeta, 512)
	return c, err
}

func (client *Client) Close() {
	client.Blobs.Close()
}

func (client *Client) RemoveCache() {
	client.Blobs.Remove()
}

// Block until the client can start the upload, thus limiting the number of file descriptor used.
func (client *Client) StartUpload() {
	client.uploaders <- struct{}{}
}

// Read from the channel to let another upload start
func (client *Client) UploadDone() {
	select {
	case <-client.uploaders:
	default:
		panic("No upload to wait for")
	}
}

// Block until the client can start the upload, thus limiting the number of file descriptor used.
func (client *Client) StartDirUpload() {
	client.dirUploaders <- struct{}{}
}

// Read from the channel to let another upload start
func (client *Client) DirUploadDone() {
	select {
	case <-client.dirUploaders:
	default:
		panic("No upload to wait for")
	}
}

func (client *Client) List() (backups []*Backup, err error) {
	con := client.Pool.Get()
	defer con.Close()
	hkeys, err := redis.Strings(con.Do("HSCAN", "backup:", "backup:\xff", 0))
	for _, hkey := range hkeys {
		meta, _ := NewBackupFromDB(client.Pool, hkey)
		backups = append(backups, meta)
	}
	return
}

func (client *Client) Put(path string) (backup *Backup, meta *Meta, wr *WriteResult, err error) {
	con := client.Pool.Get()
	_, err = con.Do("INIT")
	if err != nil {
		return
	}
	info, err := os.Stat(path)

	if os.IsNotExist(err) {
		return
	}
	var btype string
	if info.IsDir() {
		btype = "dir"
		meta, wr, err = client.PutDir(path)
	} else {
		btype = "file"
		meta, wr, err = client.PutFile(path)
	}
	if err != nil {
		return
	}
	hostname, _ := os.Hostname()
	backup = NewBackup(hostname, path, btype, meta.Hash)
	if _, err := backup.Save(client.Pool); err != nil {
		return backup, meta, wr, err
	}
	_, err = con.Do("DONE");
	return
}
