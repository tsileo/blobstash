package models

import (
	"time"
	"github.com/garyburd/redigo/redis"
	"os"
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

type Client struct {
	Pool *redis.Pool
}

func NewClient() (*Client, error) {
	pool, err := GetDbPool()
	return &Client{Pool:pool}, err
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
	return
}
