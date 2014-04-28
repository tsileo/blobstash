package models

import (
	"io/ioutil"
	"path/filepath"
	"crypto/sha1"
	"fmt"
	"github.com/garyburd/redigo/redis"
)

func (client *Client) DirWriter(path string) (wr *WriteResult, err error) {
	con := client.Pool.Get()
	defer con.Close()
	wr = &WriteResult{}
	dirdata, _ := ioutil.ReadDir(path)
	h := sha1.New()
	hashes := []string{}
	var cwr *WriteResult
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
		if cwr.Hash == "" {
			panic("Hash shouldn't be nil")
		}
		h.Write([]byte(cwr.Hash))
		hashes = append(hashes, cwr.Hash)
	}
	wr.Filename = filepath.Base(path)
	wr.Hash = fmt.Sprintf("%x", h.Sum(nil))
	_, err = con.Do("SADD", redis.Args{}.Add(wr.Hash).AddFlat(hashes)...)
	return
}

func (client *Client) PutDir(path string) (meta *Meta, wr *WriteResult, err error) {
	abspath, err := filepath.Abs(path)
	if err != nil {
		return
	}
	wr, err = client.DirWriter(abspath)
	if err != nil {
		return
	}
	meta = NewMeta()
	meta.Name = wr.Filename
	meta.Type = "dir"
	meta.Hash = wr.Hash
	err = meta.Save(client.Pool)
	return
}
