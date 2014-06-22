package client

import (
	"crypto/sha1"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"log"
	"os"
	"path/filepath"
)

// Return a slice of Meta for the directory
func (client *Client) DirIter(con redis.Conn, key string) (metas []*Meta, err error) {
	members, err := redis.Strings(con.Do("SMEMBERS", key))
	if err != nil {
		log.Printf("client: error DirIter %v SMEMBERS %v", key, err)
		//return
	}
	for _, member := range members {
		meta, merr := NewMetaFromDB(con, member)
		if merr != nil {
			log.Printf("client: error DirIter %v fetching meta %v: %v", key, member, merr)
			continue
		}
		metas = append(metas, meta)
	}
	return
}

type DirFetcher interface {
	Get(string) interface{}
}

// Used by the LRU to fetch the slice of Meta for the given dir
func (client *Client) FetchDir(con redis.Conn, key string) interface{} {
	// The error maybe be Nil if the dir is empty
	metas, _ := client.DirIter(con, key)
	//if err != nil {
	//	panic(fmt.Sprintf("Error FetchDir key:%v", key))
	//}
	return metas
}

// Reconstruct a directory given its hash to path
func (client *Client) GetDir(ctx *Ctx, key, path string) (rr *ReadResult, err error) {
	fullHash := sha1.New()
	rr = &ReadResult{}
	err = os.Mkdir(path, 0700)
	if err != nil {
		return
	}
	con := client.ConnWithCtx(ctx)
	// TODO fetch the meta with con and red the diriter
	meta, err := NewMetaFromDB(con, key)
	if err != nil {
		return nil, err
	}
	var crr *ReadResult
	if meta.Size != 0 {
		dirsMeta, err := client.DirIter(con, meta.Ref)
		if err != nil {
			return nil, fmt.Errorf("Error DirIter meta %+v: %v", meta, err)
		}
		for _, meta := range dirsMeta {
			if meta.Type == "file" {
				crr, err = client.GetFile(ctx, meta.Hash, filepath.Join(path, meta.Name))
				if err != nil {
					return rr, err
				}
			} else {
				crr, err = client.GetDir(ctx, meta.Hash, filepath.Join(path, meta.Name))
				if err != nil {
					return rr, err
				}
			}
			fullHash.Write([]byte(crr.Hash))
			rr.Add(crr)
		}
	}
	// TODO(tsileo) sum the hash and check with the root
	rr.DirsCount++
	rr.DirsDownloaded++
	rr.Hash = fmt.Sprintf("%x", fullHash.Sum(nil))
	return
}
