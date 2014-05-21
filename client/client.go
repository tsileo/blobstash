package client

import (
	"time"
	"github.com/garyburd/redigo/redis"
	"github.com/tsileo/datadatabase/lru"
	"github.com/tsileo/datadatabase/disklru"
	"os"
	"log"
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
	if err != nil {
		return nil, err
	}
	c := &Client{Pool:pool, uploader: make(chan struct{}, 100)}
	c.Blobs, err = disklru.New("./tmp_blobs_lru", c.FetchBlob, 536870912)
	c.Dirs = lru.New(c.FetchDir, 512)
	c.Metas = lru.New(c.FetchMeta, 512)
	return c, err
}

func NewTestClient() (*Client, error) {
	pool, err := GetDbPool()
	if err != nil {
		return nil, err
	}
	c := &Client{Pool:pool, uploader: make(chan struct{}, 100)}
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
	client.uploader <- struct{}{}
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
	commit := false
	con := client.Pool.Get()
	defer func() {
		if !commit {
			log.Printf("Error putting file %+v, discarding current transaction...", path)
			con.Do("TXDISCARD")
		}
		con.Close()
	}()
	txID, err := redis.String(con.Do("TXINIT"))
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
		meta, wr, err = client.PutDir(txID, path)
	} else {
		btype = "file"
		meta, wr, err = client.PutFile(txID, path)
	}
	if err != nil {
		return
	}
	backup = NewBackup(meta.Name, btype, meta.Hash)
	if _, err := backup.Save(txID, client.Pool); err != nil {
		return backup, meta, wr, err
	}	
	_, err = con.Do("TXCOMMIT")
	if err == nil {
		commit = true
	}
	return
}
