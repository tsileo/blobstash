package client

import (
	"time"
	"github.com/garyburd/redigo/redis"
	"github.com/tsileo/datadatabase/lru"
	"log"
	"os"
)

func GetDbPool() (pool *redis.Pool, err error) {
	pool = &redis.Pool{
		MaxIdle:     250,
		MaxActive: 250,
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

type Client struct {
	Pool *redis.Pool
	Blobs BlobFetcher
	Dirs DirFetcher
	Metas MetaFetcher
	uploader chan struct{}
}

func NewClient() (*Client, error) {
	pool, err := GetDbPool()
	c := &Client{Pool:pool, uploader: make(chan struct{}, 50)}
	c.Blobs = lru.New(c.FetchBlob, 512)
	c.Dirs = lru.New(c.FetchDir, 512)
	c.Metas = lru.New(c.FetchMeta, 512)
	return c, err
}

// Block until the client can start the upload
func (client *Client) StartUpload() {
	log.Println("Waiting to start upload")
	client.uploader <- struct{}{}
	log.Println("upload started")
}

// Read from the channel to let another upload start
func (client *Client) UploadDone() {
	select {
		case <-client.uploader:
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
	backup = NewBackup(meta.Name, btype, wr.Hash)
	_, err = backup.Save(client.Pool)
	if err != nil {
		panic(err)
	}
	return
}
