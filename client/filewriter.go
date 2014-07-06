package client

import (
	"bufio"
	"time"
	"crypto/sha1"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"bytes"
	"sync"
	"strconv"

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

type BlobsBuffer struct {
	thresold int
	blobs map[string]string
	done map[string]struct{}
	sync.Mutex
}

var (
	BlobsBufferDefaultThresold = 5 // Same as the number of blobs upload worker server-side
)

func NewBlobsBuffer(thresold int) *BlobsBuffer {
	if thresold == 0 {
		thresold = BlobsBufferDefaultThresold
	}
	return &BlobsBuffer{
		thresold: thresold,
		blobs: make(map[string]string),
		done: make(map[string]struct{}),
	}
}
func (b *BlobsBuffer) Put(hash, blob string) {
	b.Lock()
	defer b.Unlock()
	_, done := b.done[hash]
	if !done {
		b.blobs[hash] = blob
	}
}

func (b *BlobsBuffer) Flush(con redis.Conn, force bool) error {
	b.Lock()
	defer b.Unlock()
	if (force || len(b.blobs) >= b.thresold) && len(b.blobs) > 0 {
		hashes := []string{}
		blobs := []string{}
		for hash, _ := range b.blobs {
			hashes = append(hashes, hash)
		}
		results, err := redis.Strings(con.Do("BMULTIEXISTS", redis.Args{}.AddFlat(hashes)...))
		if err != nil {
			return err
		}
		ehashes := []string{}
		for index, res := range results {
			h := hashes[index]
			exist, err := strconv.ParseBool(res)
			if err != nil {
				return err
			}
			if !exist {
				ehashes = append(ehashes, h)
				blobs = append(blobs, b.blobs[h])
			}
			b.done[h] = struct{}{}
		}
		if len(blobs) > 0 {
			rhashes, err := redis.Strings(con.Do("BMULTIPUT", redis.Args{}.AddFlat(blobs)...))
			if err != nil {
				return err
			}
			for index, rhash := range rhashes {
				if rhash != ehashes[index] {
					return fmt.Errorf("Corrupted data: %+v/%+v", rhash, ehashes[index])
				}
			}
		}
		b.blobs = make(map[string]string)
	}
	return nil
}

// FileWriter reads the file byte and byte and upload it,
// chunk by chunk, it also constructs the file index .
func (client *Client) FileWriter(ctx *Ctx, rb *ReqBuffer, key, path string) (*WriteResult, error) {
	writeResult := NewWriteResult()
	window := 64
	rs := rolling.New(window)
	f, err := os.Open(path)
	defer f.Close()
	if err != nil {
		return nil, fmt.Errorf("can't open file %v: %v", path, err)
	}
	freader := bufio.NewReader(f)
	//if _, err := con.Do("LADD", key, 0, ""); err != nil {
	//	panic(fmt.Errorf("DB error LADD %v %v %v: %v", key, 0, "", err))
	//}
	rb.Add("ladd", key, []string{"0", ""})
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
			//ndata := string(buf.Bytes())
			fullHash.Write(buf.Bytes())
			// Check if the blob exists
			//exists, err := redis.Bool(con.Do("BEXISTS", nsha))
			exists, err := StatBlob(nsha)
			//exists, err := redis.Bool(con.Do("BEXISTS", nsha))
			if err != nil {
				panic(fmt.Sprintf("DB error: %v", err))
			}
			if !exists {
				if err := PutBlob(ctx, nsha, buf.Bytes()); err != nil {
					panic(fmt.Errorf("failed to PUT blob %v", err))
				}
				//rsha, err := redis.String(con.Do("BPUT", ndata))
				//if err != nil {
				//	panic(fmt.Sprintf("Error BPUT: %v", err))
				//}
				writeResult.BlobsUploaded++
				writeResult.SizeUploaded += buf.Len()
				// Check if the hash returned correspond to the locally computed hash
				//if rsha != nsha {
				//	panic(fmt.Sprintf("Corrupted data: %+v/%+v", rsha, nsha))
				//}
			} else {
				writeResult.SizeSkipped += buf.Len()
				writeResult.BlobsSkipped++
			}
			//if err := blobsBuffer.Flush(con, false); err != nil {
			//	panic(fmt.Errorf("failed to flush blobsBuffer: %v", err))
			//}
			//blobsBuffer.Put(nsha, ndata)
			writeResult.Size += buf.Len()
			buf.Reset()
			writeResult.BlobsCount++
			// Save the location and the blob hash into a sorted list (with the offset as index)
			rb.Add("ladd", key, []string{fmt.Sprintf("%v", writeResult.Size), nsha})
			//if _, err := con.Do("LADD", key, writeResult.Size, nsha); err != nil {
			//	panic(fmt.Errorf("DB error LADD %v %v %v: %v", key, writeResult.Size, nsha, err))
			//}
		}
		if eof {
			break
		}
	}
	writeResult.Hash = fmt.Sprintf("%x", fullHash.Sum(nil))
	writeResult.FilesCount++
	writeResult.FilesUploaded++
	return writeResult, nil
}

func (client *Client) SmallFileWriter(ctx *Ctx, rb *ReqBuffer, key, path string) (*WriteResult, error) {
	//log.Printf("start:%v / %v", time.Now(), path)
	writeResult := NewWriteResult()
	f, err := os.Open(path)
	defer f.Close()
	if err != nil {
		return nil, fmt.Errorf("can't open file %v: %v", path, err)
	}
	rb.Add("ladd", key, []string{"0", ""})
	buf2, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %v: %v", path, err)
	}
	nsha := SHA1(buf2)
	//ndata := string(buf2)
	//if err := blobsBuffer.Flush(con, false); err != nil {
	//	panic(fmt.Errorf("failed to flush blobsBuffer: %v", err))
	//}
	//blobsBuffer.Put(nsha, ndata)
	//exists, err := redis.Bool(con.Do("BEXISTS", nsha))
	exists, err := StatBlob(nsha)
	if err != nil {
		panic(fmt.Sprintf("DB error: %v", err))
	}
	if !exists {
		if err := PutBlob(ctx, nsha, buf2); err != nil {
			panic(fmt.Errorf("failed to PUT blob %v", err))
		}
		//rsha, err := redis.String(con.Do("BPUT", ndata))
		//if err != nil {
		//	panic(fmt.Sprintf("Error BPUT: %v", err))
		//}
		writeResult.BlobsUploaded++
		writeResult.SizeUploaded += len(buf2)
		// Check if the hash returned correspond to the locally computed hash
		//if rsha != nsha {
		//	panic(fmt.Sprintf("Corrupted data: %+v/%+v", rsha, nsha))
		//}
	} else {
		writeResult.SizeSkipped += len(buf2)
		writeResult.BlobsSkipped++
	}
	writeResult.Size += len(buf2)
	writeResult.BlobsCount++
	// Save the location and the blob hash into a sorted list (with the offset as index)
	rb.Add("ladd", key, []string{fmt.Sprintf("%v", writeResult.Size), nsha})
	writeResult.Hash = nsha
	writeResult.FilesCount++
	writeResult.FilesUploaded++
	return writeResult, nil
}

func (client *Client) PutFile(ctx *Ctx, rb *ReqBuffer, path string) (*Meta, *WriteResult, error) {
	wr := NewWriteResult()
	client.StartUpload()
	defer client.UploadDone()
	fstat, err := os.Stat(path)
	if os.IsNotExist(err) {
		return nil, nil, err
	}
	_, filename := filepath.Split(path)
	sha := FullSHA1(path)
	con := client.ConnWithCtx(ctx)
	defer con.Close()
	newRb := false
	if rb == nil {
		newRb = true
		rb = NewReqBuffer()
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
			wr, err = client.FileWriter(ctx, rb, sha, path)
		} else {
			wr, err = client.SmallFileWriter(ctx, rb, sha, path)
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
	if err := meta.SaveToBuffer(con, rb); err != nil {
		return nil, nil, fmt.Errorf("Error saving meta %+v: %v", meta, err)
	}
	if newRb {
		rb.Lock()
		defer rb.Unlock()
		mbhash, mblob := rb.JSON()
		//_, err = con.Do("MBPUT", mblob)
		if err := PutBlob(&Ctx{MetaBlob: true, Namespace: ctx.Namespace}, mbhash, mblob); err != nil {
			return nil, nil, fmt.Errorf("error MBPUT: %+v", err)
		}
	}
	return meta, wr, nil
}
