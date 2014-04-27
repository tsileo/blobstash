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


func FileWriter(key, path string) (*WriteResult, error) {
	writeResult := &WriteResult{}
	window := 64
	rs := rolling.New(window)
	f, err := os.Open(path)
	defer f.Close()
	if err != nil {
		return writeResult, err
	}
	freader := bufio.NewReader(f)
	rpool, _ := GetDbPool()
	con := rpool.Get()
	defer con.Close()
	var buf bytes.Buffer
	buf.Reset()
	fullHash := sha1.New()
	eof := false
	for {
		b := make([]byte, 1)
		_, err := freader.Read(b)
		if err == io.EOF {
			eof = true
		} else {
			rs.Write(b)
			buf.Write(b)	
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
			con.Do("LADD", key, writeResult.BlobsCnt, nsha)
			writeResult.Size += buf.Len()
			buf.Reset()
			writeResult.BlobsCnt++
		}
		if eof {
			break
		}
	}
	writeResult.Hash = fmt.Sprintf("%x", fullHash.Sum(nil))
	return writeResult, nil
}

func RawPutFile(path string) (wr *WriteResult, err error) {
	if _, err = os.Stat(path); os.IsNotExist(err) {
		return
	}
	sha := FullSHA1(path)
	wr, err = FileWriter(sha, path)
	_, filename := filepath.Split(path)
	wr.Filename = filename
	return
}