package models

import (
	"io/ioutil"
	"path/filepath"
	"crypto/sha1"
	"fmt"
	"log"
	"sort"
	"github.com/garyburd/redigo/redis"
)

func (client *Client) DirWriter(path string) (wr *WriteResult, err error) {
	con := client.Pool.Get()
	defer con.Close()
	wr = &WriteResult{Filename: filepath.Base(path)}
	dirdata, err := ioutil.ReadDir(path)
	if err != nil {
		return
	}
	h := sha1.New()
	hashes := []string{}
	var cwr *WriteResult
	if len(dirdata) != 0 {
		for _, data := range dirdata {
			abspath := filepath.Join(path, data.Name())
			if data.IsDir() {
				_, cwr, err = client.PutDir(abspath)
			} else {
				_, cwr, err = client.PutFile(abspath)
			}
			if err != nil {
				return
			}
			wr.Add(cwr)
			hashes = append(hashes, cwr.Hash)
		}
		sort.Strings(hashes)
		for _, hash := range hashes {
			h.Write([]byte(hash))
		}
		wr.Hash = fmt.Sprintf("%x", h.Sum(nil))
		cnt, err := redis.Int(con.Do("SCARD", wr.Hash))
		if err != nil {
			return wr, err
		}
		if cnt == 0 {
			_, err = con.Do("SADD", redis.Args{}.Add(wr.Hash).AddFlat(hashes)...)
			if err != nil {
				return wr, err
			}
		} else {
			wr.AlreadyExists = true
		}
	} else {
		// If the dir is empty, hash the filename instead of members
		// so it doesn't break things.
		h.Write([]byte("emptydir:"))
		h.Write([]byte(wr.Filename))
		wr.Hash = fmt.Sprintf("%x", h.Sum(nil))
	}
	//log.Printf("datadb: DirWriter(%v) WriteResult:%+v", path, wr)
	return
}

func (client *Client) PutDir(path string) (meta *Meta, wr *WriteResult, err error) {
	abspath, err := filepath.Abs(path)
	if err != nil {
		return
	}
	wr, err = client.DirWriter(abspath)
	if err != nil {
		log.Printf("datadb: error DirWriter path:%v/abspath:%v/wr:%+v/err:%v\n", path, abspath, wr, err)
		return
	}
	meta = NewMeta()
	meta.Name = wr.Filename
	meta.Type = "dir"
	meta.Size = wr.Size
	meta.Hash = wr.Hash
	err = meta.Save(client.Pool)
	return
}
