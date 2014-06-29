package client

import (
	"bufio"
	"time"
	"crypto/sha1"
	"errors"
	"fmt"
	"log"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"bytes"

	"github.com/garyburd/redigo/redis"
	"github.com/tsileo/blobstash/rolling"
)

var (
	MinBlobSize = 64<<10 // 64Kb
	MaxBlobSize = 1<<20 // 1MB
)

var (
	emptyHash = "da39a3ee5e6b4b0d3255bfef95601890afd80709"
)

// FileWriter reads the file byte and byte and upload it,
// chunk by chunk, it also constructs the file index .
func (client *Client) FileWriter(con redis.Conn, key, path string) (*WriteResult, error) {
	writeResult := NewWriteResult()
	window := 64
	rs := rolling.New(window)
	f, err := os.Open(path)
	defer f.Close()
	if err != nil {
		return nil, fmt.Errorf("can't open file %v: %v", path, err)
	}
	freader := bufio.NewReader(f)
	if _, err := con.Do("LADD", key, 0, ""); err != nil {
		panic(fmt.Errorf("DB error LADD %v %v %v: %v", key, 0, "", err))
	}
	//log.Printf("FileWriter(%v, %v, %v)", txID, key, path)
	//buf := client.bufferPool.Get().(*bytes.Buffer)
	var buf bytes.Buffer
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
		if (onSplit && (buf.Len() > MinBlobSize)) || buf.Len() >= MaxBlobSize || eof {
			nsha := SHA1(buf.Bytes())
			ndata := string(buf.Bytes())
			fullHash.Write(buf.Bytes())
			// Check if the blob exists
			exists, err := redis.Bool(con.Do("BEXISTS", nsha))
			if err != nil {
				panic(fmt.Sprintf("DB error: %v", err))
			}
			if !exists {
				rsha, err := redis.String(con.Do("BPUT", ndata))
				if err != nil {
					panic(fmt.Sprintf("Error BPUT: %v", err))
				}
				writeResult.BlobsUploaded++
				writeResult.SizeUploaded += buf.Len()
				// Check if the hash returned correspond to the locally computed hash
				if rsha != nsha {
					panic(fmt.Sprintf("Corrupted data: %+v/%+v", rsha, nsha))
				}
			} else {
				writeResult.SizeSkipped += buf.Len()
				writeResult.BlobsSkipped++
			}
			writeResult.Size += buf.Len()
			buf.Reset()
			writeResult.BlobsCount++
			// Save the location and the blob hash into a sorted list (with the offset as index)
			if _, err := con.Do("LADD", key, writeResult.Size, nsha); err != nil {
				panic(fmt.Errorf("DB error LADD %v %v %v: %v", key, writeResult.Size, nsha, err))
			}
		}
		if eof {
			break
		}
	}
	writeResult.Hash = fmt.Sprintf("%x", fullHash.Sum(nil))
	writeResult.FilesCount++
	writeResult.FilesUploaded++
	// Returns the buffer to the pool
	//client.putBuffer(buf)
	log.Printf("FileWriter %v %v done", key, path)
	return writeResult, nil
}

func (client *Client) SmallFileWriter(con redis.Conn, key, path string) (*WriteResult, error) {
	//log.Printf("start:%v / %v", time.Now(), path)
	writeResult := NewWriteResult()
	f, err := os.Open(path)
	defer f.Close()
	if err != nil {
		return nil, fmt.Errorf("can't open file %v: %v", path, err)
	}
	if _, err := con.Do("LADD", key, 0, ""); err != nil {
		panic(fmt.Errorf("DB error LADD %v %v %v: %v", key, 0, "", err))
	}
	buf2, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %v: %v", path, err)
	}
	nsha := SHA1(buf2)
	ndata := string(buf2)
	exists, err := redis.Bool(con.Do("BEXISTS", nsha))
	if err != nil {
		panic(fmt.Sprintf("DB error: %v", err))
	}
	if !exists {
		rsha, err := redis.String(con.Do("BPUT", ndata))
		if err != nil {
			panic(fmt.Sprintf("Error BPUT: %v", err))
		}
		writeResult.BlobsUploaded++
		writeResult.SizeUploaded += len(buf2)
		// Check if the hash returned correspond to the locally computed hash
		if rsha != nsha {
			panic(fmt.Sprintf("Corrupted data: %+v/%+v", rsha, nsha))
		}
	} else {
		writeResult.SizeSkipped += len(buf2)
		writeResult.BlobsSkipped++
	}
	writeResult.Size += len(buf2)
	writeResult.BlobsCount++
	// Save the location and the blob hash into a sorted list (with the offset as index)
	if _, err := con.Do("LADD", key, writeResult.Size, nsha); err != nil {
		panic(fmt.Errorf("DB error LADD %v %v %v: %v", key, writeResult.Size, nsha, err))
	}
	writeResult.Hash = nsha
	writeResult.FilesCount++
	writeResult.FilesUploaded++
	log.Printf("SmallFileWriter %v %v done", key, path)
	//log.Printf("end: %v", time.Now())
	return writeResult, nil
}

func (client *Client) PutFile(ctx *Ctx, path string) (*Meta, *WriteResult, error) {
	wr := NewWriteResult()
	//log.Printf("PutFile %+v/%v\n", ctx, path)
	client.StartUpload()
	defer client.UploadDone()
	fstat, err := os.Stat(path)
	if os.IsNotExist(err) {
		return nil, nil, err
	}
	_, filename := filepath.Split(path)
	sha := FullSHA1(path)
	con := client.Conn()
	defer con.Close()

	_, err = redis.String(con.Do("TXINIT", ctx.Args()...))
	if err != nil {
		return nil, nil, err
	}

	// First we check if the file isn't already uploaded,
	// if so we skip it.
	cnt, err := redis.Int(con.Do("LLEN", sha))
	if err != nil {
		return nil, nil, fmt.Errorf("error LLEN %v: %v", sha, err)
	}
	if cnt > 0 || sha == emptyHash {
		wr.Hash = sha
		wr.AlreadyExists = true
		wr.FilesSkipped++
		wr.FilesCount++
		wr.SizeSkipped = int(fstat.Size())
		wr.Size = wr.SizeSkipped
		wr.BlobsCount += cnt
		wr.BlobsSkipped += cnt
	} else {
		if int(fstat.Size()) > MinBlobSize {
			wr, err = client.FileWriter(con, sha, path)
		} else {
			wr, err = client.SmallFileWriter(con, sha, path)
		}
		if err != nil {
			return nil, nil, fmt.Errorf("FileWriter %v error: %v", path, err)
		}
	}
	meta := NewMeta()
	if sha != wr.Hash {
		err = errors.New("initial hash and WriteResult aren't the same")
		return nil, nil, err
	}
	meta.Ref = wr.Hash
	meta.Name = filename
	//if wr.Size != fstat.Size()
	meta.Size = wr.Size
	meta.Type = "file"
	meta.ModTime = fstat.ModTime().Format(time.RFC3339)
	meta.Mode = uint32(fstat.Mode())
	if err := meta.Save(con); err != nil {
		return nil, nil, fmt.Errorf("Error saving meta %+v: %v", meta, err)
	}
	_, err = con.Do("TXCOMMIT")
	if err != nil {
		return nil, nil, fmt.Errorf("error TXCOMMIT: %+v", err)
	}
	return meta, wr, nil
}
