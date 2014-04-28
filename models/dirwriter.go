package models

import (
	"io/ioutil"
	"path/filepath"
	"crypto/sha1"
	"fmt"
)

func (client *Client) DirWriter(path string) (wr *WriteResult, err error) {
	con := client.Pool.Get()
	defer con.Close()
	wr = &WriteResult{}
	dirdata, _ := ioutil.ReadDir(path)
	h := sha1.New()
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
		h.Write([]byte(wr.Hash))
	}
	wr.Filename = filepath.Dir(path) 
	wr.Hash = fmt.Sprintf("%x", h.Sum(nil))
	return wr, nil
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
