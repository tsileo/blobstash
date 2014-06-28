package client

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
	"sync"
	"bytes"

	"github.com/garyburd/redigo/redis"

	"github.com/tsileo/blobstash/disklru"
	"github.com/tsileo/blobstash/lru"
)

var (
	uploaders    = 50 // concurrent upload uploaders
	dirUploaders = 25 // concurrent directory uploaders
)


type Client struct {
	Pool         *redis.Pool
	Hostname     string
	Blobs        *disklru.DiskLRU
	Dirs         *lru.LRU
	Metas        *lru.LRU
	uploaders    chan struct{}
	dirUploaders chan struct{}
	bufferPool *sync.Pool
	ignoredFiles []string
}

type Ctx struct {
	Hostname string
	Archive  bool
}

func (ctx *Ctx) Args() redis.Args {
	return redis.Args{}.Add(ctx.Hostname).Add(ctx.Archive)
}

func NewBuffer() interface{} {
	var b bytes.Buffer
	return b
}

func NewClient(hostname string, ignoredFiles []string) (*Client, error) {
	var err error
	if hostname == "" {
		hostname, err = os.Hostname()
		if err != nil {
			return nil, err
		}
	}
	c := &Client{
		Hostname: hostname,
		uploaders: make(chan struct{}, uploaders),
		dirUploaders: make(chan struct{}, dirUploaders),
		bufferPool: &sync.Pool{
			New: NewBuffer,
		},
	}
	if err := c.SetupPool(); err != nil {
		return nil, err
	}
	c.Blobs, err = disklru.New("", c.FetchBlob, 536870912)
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

func (c *Client) getBuffer() bytes.Buffer {
	buf := c.bufferPool.Get().(bytes.Buffer)
	return buf
}

func (c *Client) putBuffer(buf bytes.Buffer) {
	buf.Reset()
	c.bufferPool.Put(buf)
}

func (client *Client) SetupPool() error {
	client.Pool = &redis.Pool{
		MaxIdle:     50,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", "localhost:9735")
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
	return nil
}

func (client *Client) Conn() redis.Conn {
	return client.Pool.Get()
}

func (client *Client) ConnWithCtx(ctx *Ctx) redis.Conn {
	con := client.Pool.Get()
	con.Do("SETCTX", ctx.Args()...)
	return con
}

func NewTestClient(hostname string) (*Client, error) {
	var err error
	if hostname == "" {
		hostname, err = os.Hostname()
		if err != nil {
			return nil, err
		}
	}
	c := &Client{Hostname: hostname,
		uploaders: make(chan struct{}, uploaders),
		dirUploaders: make(chan struct{}, dirUploaders)}
	c.Blobs, err = disklru.NewTest(c.FetchBlob, 536870912)
	c.Dirs = lru.New(c.FetchDir, 512)
	c.Metas = lru.New(c.FetchMeta, 512)
	c.SetupPool()
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

//func (client *Client) List() (backups []*Backup, err error) {
//	con := client.Pool.Get()
//	defer con.Close()
//	hkeys, err := redis.Strings(con.Do("HSCAN", "backup:", "backup:\xff", 0))
//	for _, hkey := range hkeys {
//		meta, _ := NewBackupFromDB(client.Pool, hkey)
//		backups = append(backups, meta)
//	}
//	return
//}

func (client *Client) Get(hash, path string) (snapshot *Snapshot, meta *Meta, rr *ReadResult, err error) {
	con := client.Conn()
	defer con.Close()
	host, err := redis.String(con.Do("GET", hash))
	if err != nil {
		return
	}
	ctx := &Ctx{Hostname: host}
	_, err = con.Do("SETCTX", ctx.Args()...)
	if err != nil {
		return
	}
	snapshot, err = NewSnapshotFromDB(con, hash)
	if err != nil {
		return
	}
	meta, err = snapshot.Meta(con)
	if err != nil {
		return
	}
	switch {
	case snapshot.Type == "dir":
		rr, err = client.GetDir(ctx, snapshot.Ref, path)
 		if err != nil {
 			return
 		}
	case snapshot.Type == "file":
 		rr, err = client.GetFile(ctx, snapshot.Ref, path)
 		if err != nil {
 			return
 		}
	default:
		err = fmt.Errorf("unknow meta type %v for snapshot %+v", snapshot.Type, snapshot)
		return
	}
	return
}

func (client *Client) Put(ctx *Ctx, path string) (snapshot *Snapshot, meta *Meta, wr *WriteResult, err error) {
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return
	}
	var btype string
	if info.IsDir() {
		btype = "dir"
		meta, wr, err = client.PutDir(ctx, path)
	} else {
		btype = "file"
		meta, wr, err = client.PutFile(ctx, path)
	}
	if err != nil {
		return
	}
	con := client.Conn()
	defer con.Close()
	if _, err = redis.String(con.Do("TXINIT", ctx.Args()...)); err != nil {
		return
	}
	snapshot = NewSnapshot(client.Hostname, path, btype, meta.Hash)
	saver := snapshot.Save
	if ctx.Archive {
		saver = snapshot.SaveAsArchive
	}
	if err := saver(con); err != nil {
		return snapshot, meta, wr, err
	}
	_, err = con.Do("TXCOMMIT")
	return
}
