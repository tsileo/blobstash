package models

import (
	"log"
	"github.com/garyburd/redigo/redis"
	"os"
	"path/filepath"
)

func (client *Client) GetDir(key, path string) (rr *ReadResult, err error) {
	rr = &ReadResult{}
	con := client.Pool.Get()
	defer con.Close()
	members, err := redis.Strings(con.Do("SMEMBERS", key))
	if err != nil {
		return
	}
	err = os.Mkdir(path, 0700)
	if err != nil {
		return
	}
	for _, member := range members {
		meta, _ := NewMetaFromDB(client.Pool, member)
		if meta.Type == "file" {
			_, err = client.GetFile(meta.Hash, filepath.Join(path, meta.Name))
			if err != nil {
				panic(err)
			}
			log.Printf("file:%+v", meta)
		} else {
			_, err = client.GetDir(meta.Hash, filepath.Join(path, meta.Name))
			if err != nil {
				panic(err)
			}
			log.Printf("dir:%+v", meta)
		}
	}
	return
}
