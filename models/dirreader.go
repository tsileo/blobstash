package models

import (
	"github.com/garyburd/redigo/redis"
	"os"
	"path/filepath"
	"fmt"
	"crypto/sha1"
)

func (client *Client) GetDir(key, path string) (rr *ReadResult, err error) {
	fullHash := sha1.New()
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
	var crr *ReadResult
	for _, member := range members {
		meta, _ := NewMetaFromDB(client.Pool, member)
		if meta.Type == "file" {
			crr, err = client.GetFile(meta.Hash, filepath.Join(path, meta.Name))
			if err != nil {
				return
			}
		} else {
			crr, err = client.GetDir(meta.Hash, filepath.Join(path, meta.Name))
			if err != nil {
				return
			}
		}
		fullHash.Write([]byte(crr.Hash))
		rr.Add(crr)

	}
	// TODO(ts) sum the hash and check with the root
	rr.Hash = fmt.Sprintf("%x", fullHash.Sum(nil))
	return
}
