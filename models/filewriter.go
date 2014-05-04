package models

import (
	"os"
	"bytes"
	"crypto/sha1"
	"fmt"
	"io"
	"bufio"
	"github.com/tsileo/silokv/rolling"
	"github.com/garyburd/redigo/redis"
	"path/filepath"
)

func (client *Client) FileWriter(key, path string) (*WriteResult, error) {
	writeResult := &WriteResult{}
	window := 64
	rs := rolling.New(window)
	f, err := os.Open(path)
	defer f.Close()
	if err != nil {
		return writeResult, err
	}
	freader := bufio.NewReader(f)
	con := client.Pool.Get()
	defer con.Close()
	var buf bytes.Buffer
	buf.Reset()
	fullHash := sha1.New()
	eof := false
	i := 0
	for {
		b := make([]byte, 1)
		_, err := freader.Read(b)
		if err == io.EOF {
			eof = true
		} else {
			rs.Write(b)
			buf.Write(b)
			i++
		}
		onSplit := rs.OnSplit()
		if (onSplit && (buf.Len() > 64 << 10)) || buf.Len() >= 1 << 20 || eof {
			nsha := SHA1(buf.Bytes())
			ndata := string(buf.Bytes())
			fullHash.Write(buf.Bytes())
			exists, err := redis.Bool(con.Do("BEXISTS", nsha))
			if err != nil {
				panic(fmt.Sprintf("DB error: %v", err))
			}
			if !exists {
				rsha, err := redis.String(con.Do("BPUT", ndata))
				if err != nil {
					panic(fmt.Sprintf("DB error: %v", err))
				}
				writeResult.UploadedCnt++
				writeResult.UploadedSize += buf.Len()
				if rsha != nsha {
					panic(fmt.Sprintf("Corrupted data: %+v/%+v", rsha, nsha))
				}
			} else {
				writeResult.SkippedSize += buf.Len()
				writeResult.SkippedCnt++
			}
			writeResult.Size += buf.Len()
			buf.Reset()
			writeResult.BlobsCnt++
			con.Do("LADD", key, writeResult.Size, nsha)
			
		}
		if eof {
			break
		}
	}
	writeResult.Hash = fmt.Sprintf("%x", fullHash.Sum(nil))
	return writeResult, nil
}

func (client *Client) PutFile(path string) (meta *Meta, wr *WriteResult, err error) {
	if _, err = os.Stat(path); os.IsNotExist(err) {
		return
	}
	_, filename := filepath.Split(path)
	sha := FullSHA1(path)
	con := client.Pool.Get()
	defer con.Close()
	cnt, err := redis.Int(con.Do("HLEN", sha))
	if err != nil {
		return
	}
	if cnt > 0 {
		wr = &WriteResult{}
		wr.Hash = sha
		wr.AlreadyExists = true
		wr.Filename = filename
	} else {
		wr, err = client.FileWriter(sha, path)
		if err != nil {
			return
		}	
	}
	meta = NewMeta()
	if sha != wr.Hash {
		panic("Corrupted")
	}
	meta.Hash = wr.Hash
	meta.Name = filename
	meta.Size = wr.Size
	meta.Type = "file"
	err = meta.Save(client.Pool)
	return
}