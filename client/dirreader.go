package client

import (
	"github.com/garyburd/redigo/redis"
	"os"
	"path/filepath"
	"fmt"
	"log"
	"crypto/sha1"
)

// Return a slice of Meta for the directory
func (client *Client) DirIter(key string) (metas []*Meta, err error) {
	con := client.Pool.Get()
	defer con.Close()
	members, err := redis.Strings(con.Do("SMEMBERS", key))
	if err != nil {
		log.Printf("client: error DirIter SMEMBERS %v", err)
		return
	}
	for _, member := range members {
		meta, merr := NewMetaFromDB(client.Pool, member)
		if merr != nil {
			log.Printf("client: error DirIter fetching meta %v", member)
			continue
		}
		metas = append(metas, meta)
	}
	return
}

type DirFetcher interface{
	Get(string) interface{}
}

// Used by the LRU to fetch the slice of Meta for the given dir
func (client *Client) FetchDir(key string) interface{} {
	// The error maybe be Nil if the dir is empty
	metas, _ := client.DirIter(key)
	//if err != nil {
	//	panic(fmt.Sprintf("Error FetchDir key:%v", key))
	//}
	return metas
}

// Reconstruct a directory given its hash to path
func (client *Client) GetDir(key, path string) (rr *ReadResult, err error) {
	fullHash := sha1.New()
	rr = &ReadResult{}
	err = os.Mkdir(path, 0700)
	if err != nil {
		return
	}
	var crr *ReadResult
	for _, meta := range client.Dirs.Get(key).([]*Meta) {
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
	// TODO(tsileo) sum the hash and check with the root
	rr.DirsCount++
	rr.DirsDownloaded++
	rr.Hash = fmt.Sprintf("%x", fullHash.Sum(nil))
	return
}
