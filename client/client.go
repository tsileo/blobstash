package client

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/tsileo/blobstash/disklru"
	"github.com/tsileo/blobstash/lru"
	"os"
	"path/filepath"
	"time"
)

var (
	uploaders    = 25 // concurrent upload uploaders
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
	ignoredFiles []string
}

type Ctx struct {
	Hostname string
	Archive  bool
}

func (ctx *Ctx) Args() redis.Args {
	return redis.Args{}.Add(ctx.Hostname).Add(ctx.Archive)
}

func NewClient(hostname string, ignoredFiles []string) (*Client, error) {
	var err error
	if hostname == "" {
		hostname, err = os.Hostname()
		if err != nil {
			return nil, err
		}
	}
	c := &Client{Hostname: hostname, uploaders: make(chan struct{}, uploaders),
		dirUploaders: make(chan struct{}, dirUploaders)}
	if err := c.SetupPool(); err != nil {
		return nil, err
	}
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
	con := client.ConnWithCtx(&Ctx{Hostname: client.Hostname})
	defer con.Close()
	snapshot, err = NewSnapshotFromDB(con, hash)
	if err != nil {
		return
	}
	// TODO(tsileo) find a better way to handle Ctx on Get
	ctx := &Ctx{Hostname: snapshot.Hostname}
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
	if err := snapshot.Save(con); err != nil {
		return snapshot, meta, wr, err
	}
	_, err = con.Do("TXCOMMIT")
	return
}
