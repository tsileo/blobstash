package uploader

import (
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

// FileWriter reads the file byte and byte and upload it,
// chunk by chunk, it also constructs the file index .
func (up *Uploader) FileWriter(cctx *ctx.Ctx, tx *client2.Transaction, key, path string) (*WriteResult, error) {
	writeResult := NewWriteResult()
	// Init the rolling checksum
	window := 64
	rs := rolling.New(window)
	// Open the file
	f, err := os.Open(path)
	defer f.Close()
	if err != nil {
		return writeResult, fmt.Errorf("can't open file %v: %v", path, err)
	}
	// Prepare the reader to compute the hash on the fly
	fullHash := sha1.New()
	freader := io.TeeReader(f, fullHash)
	// Init the list that wil hold blobs reference
	tx.Ladd(key, 0, "")

	eof := false
	i := 0
	
	// Prepare the blob writer
	var buf bytes.Buffer
	blobHash := sha1.New()
	blobWriter := io.MultiWriter(&buf, blobHash, rs)
	for {
		b := make([]byte, 1)
		_, err := freader.Read(b)
		if err == io.EOF {
			eof = true
		} else {
			blobWriter.Write(b)
			i++
		}
		onSplit := rs.OnSplit()
		if (onSplit && (buf.Len() > MinBlobSize)) || buf.Len() >= MaxBlobSize || eof {
			nsha := fmt.Sprintf("%x", blobHash.Sum(nil))
			// Check if the blob exists
			exists, err := up.client.BlobStore.Stat(cctx, nsha)
			if err != nil {
				panic(fmt.Sprintf("DB error: %v", err))
			}
			if !exists {
				if err := up.client.BlobStore.Put(cctx, nsha, buf.Bytes()); err != nil {
					panic(fmt.Errorf("failed to PUT blob %v", err))
				}
				writeResult.BlobsUploaded++
				writeResult.SizeUploaded += buf.Len()
			} else {
				writeResult.SizeSkipped += buf.Len()
				writeResult.BlobsSkipped++
			}
			writeResult.Size += buf.Len()
			buf.Reset()
			blobHash.Reset()
			writeResult.BlobsCount++
			// Save the location and the blob hash into a sorted list (with the offset as index)
			tx.Ladd(key, writeResult.Size, nsha)
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

func (up *Uploader) PutFile(cctx *ctx.Ctx, tx *client2.Transaction, path string) (*Meta, *WriteResult, error) {
	fstat, err := os.Stat(path)
	if os.IsNotExist(err) {
		return nil, nil, err
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
		return nil, nil, fmt.Errorf("error LLEN %v: %v", sha, err)
	}
	wr := NewWriteResult()
	if cnt > 0 || fstat.Size() == 0 {
		wr.Hash = sha
		wr.AlreadyExists = true
		wr.FilesSkipped++
		wr.FilesCount++
		wr.SizeSkipped = int(fstat.Size())
		wr.Size = wr.SizeSkipped
		wr.BlobsCount += cnt
		wr.BlobsSkipped += cnt
	}
	if cnt == 0 {
		cwr, err := up.FileWriter(cctx, tx, sha, path)
		if err != nil {
			return nil, nil, err
		}
		wr.free()
		wr = cwr
	}
	meta := NewMeta()
	meta.Ref = sha
	meta.Name = filename
	meta.Size = int(fstat.Size())
	meta.Type = "file"
	meta.ModTime = fstat.ModTime().Format(time.RFC3339)
	meta.Mode = uint32(fstat.Mode())
	meta.ComputeHash()
	tx.Hmset(meta.Hash, client2.FormatStruct(meta)...)
	if newTx {
		if err := up.client.Commit(cctx, tx); err != nil {
			return meta, wr, err
		}
	}
	return meta, wr, nil
}
