package clientutil

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/dchest/blake2b"

	"github.com/tsileo/blobstash/client"
	"github.com/tsileo/blobstash/client/transaction"
	"github.com/tsileo/blobstash/client/ctx"
	"github.com/tsileo/blobstash/rolling"
)

var (
	MinBlobSize = 64 << 10 // 64Kb
	MaxBlobSize = 1 << 20  // 1MB
)

// FileWriter reads the file byte and byte and upload it,
// chunk by chunk, it also constructs the file index .
func (up *Uploader) FileWriter(cctx *ctx.Ctx, tx *transaction.Transaction, key, path string) (*WriteResult, error) {
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
	fullHash := blake2b.New256()
	freader := io.TeeReader(f, fullHash)
	eof := false
	i := 0
	// Prepare the blob writer
	var buf bytes.Buffer
	blobHash := blake2b.New256()
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

func (up *Uploader) PutFile(cctx *ctx.Ctx, tx *transaction.Transaction, path string) (*Meta, *WriteResult, error) {
	up.StartUpload()
	defer up.UploadDone()
	fstat, err := os.Stat(path)
	if os.IsNotExist(err) {
		return nil, nil, err
	}
	_, filename := filepath.Split(path)
	sha := FullHash(path)
	con := up.client.ConnWithCtx(cctx)
	defer con.Close()
	newTx := false
	if tx == nil {
		newTx = true
		tx = client.NewTransaction()
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
	hlen, err := up.client.Hlen(con, meta.Hash)
	if err != nil {
		return nil, nil, err
	}
	if hlen == 0 {
		tx.Hmset(meta.Hash, client.FormatStruct(meta)...)
	}
	if newTx {
		if err := up.client.Commit(cctx, tx); err != nil {
			return meta, wr, err
		}
	}
	return meta, wr, nil
}
