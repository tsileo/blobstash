package uploader

import (
	"bufio"
	"time"
	"crypto/sha1"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"bytes"

	"github.com/tsileo/blobstash/rolling"
	"github.com/tsileo/blobstash/client2/ctx"
	"github.com/tsileo/blobstash/client2"
)

var (
	MinBlobSize = 64<<10 // 64Kb
	MaxBlobSize = 1<<20 // 1MB
)

// SHA1 is a helper to quickly compute the SHA1 hash of a []byte.
func SHA1(data []byte) string {
	h := sha1.New()
	h.Write(data)
	return fmt.Sprintf("%x", h.Sum(nil))
}

// FullSHA1 is helper to compute the SHA1 of the given file path.
func FullSHA1(path string) string {
	f, _ := os.Open(path)
	defer f.Close()
	reader := bufio.NewReader(f)
	h := sha1.New()
	_, _ = io.Copy(h, reader)
	return fmt.Sprintf("%x", h.Sum(nil))
}

// FileWriter reads the file byte and byte and upload it,
// chunk by chunk, it also constructs the file index .
func (up *Uploader) FileWriter(cctx *ctx.Ctx, tx *client2.Transaction, key, path string) (error) {
	window := 64
	rs := rolling.New(window)
	f, err := os.Open(path)
	defer f.Close()
	if err != nil {
		return fmt.Errorf("can't open file %v: %v", path, err)
	}
	freader := bufio.NewReader(f)
	tx.Ladd(key, 0, "")
	var buf bytes.Buffer
	fullHash := sha1.New()
	eof := false
	i := 0
	size := 0
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
			fullHash.Write(buf.Bytes())
			// Check if the blob exists
			exists, err := up.client.BlobStore.Stat(cctx, nsha)
			if err != nil {
				panic(fmt.Sprintf("DB error: %v", err))
			}
			if !exists {
				if err := up.client.BlobStore.Put(cctx, nsha, buf.Bytes()); err != nil {
					panic(fmt.Errorf("failed to PUT blob %v", err))
				}
				//writeResult.BlobsUploaded++
				//writeResult.SizeUploaded += buf.Len()
				// Check if the hash returned correspond to the locally computed hash
				//if rsha != nsha {
				//	panic(fmt.Sprintf("Corrupted data: %+v/%+v", rsha, nsha))
				//}
			}
			//} else {
			//	writeResult.SizeSkipped += buf.Len()
			//	writeResult.BlobsSkipped++
			//}
			//writeResult.Size += buf.Len()
			size += buf.Len()
			buf.Reset()
			//writeResult.BlobsCount++
			// Save the location and the blob hash into a sorted list (with the offset as index)
			tx.Ladd(key, size, nsha)
		}
		if eof {
			break
		}
	}
	//writeResult.Hash = fmt.Sprintf("%x", fullHash.Sum(nil))
	//writeResult.FilesCount++
	//writeResult.FilesUploaded++
	return nil
}

func (up *Uploader) PutFile(cctx *ctx.Ctx, tx *client2.Transaction, path string) (*Meta, error) {
	fstat, err := os.Stat(path)
	if os.IsNotExist(err) {
		return nil, err
	}
	_, filename := filepath.Split(path)
	sha := FullSHA1(path)
	con := up.client.ConnWithCtx(cctx)
	defer con.Close()
	newTx := false
	if tx == nil {
		newTx = true
		tx = client2.NewTransaction()
	}
	// First we check if the file isn't already uploaded,
	// if so we skip it.
	cnt, err := up.client.Llen(con, sha)
	if err != nil {
		return nil, fmt.Errorf("error LLEN %v: %v", sha, err)
	}
	if cnt == 0 {
		if err := up.FileWriter(cctx, tx, sha, path); err != nil {
			return nil, err
		}	
	}
	meta := NewMeta()
	// TODO test the filewriter hash ?
	meta.Ref = sha
	meta.Name = filename
	//if wr.Size != fstat.Size()
	meta.Size = int(fstat.Size())
	meta.Type = "file"
	meta.ModTime = fstat.ModTime().Format(time.RFC3339)
	meta.Mode = uint32(fstat.Mode())
	meta.ComputeHash()
	tx.Hmset(meta.Hash, client2.FormatStruct(meta)...)
	if newTx {
		if err := up.client.Commit(cctx, tx); err != nil {
			return nil, err
		}
	}
	return meta, nil
}
