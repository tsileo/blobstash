package writer // import "a4.io/blobstash/pkg/filetree/writer"

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/dchest/blake2b"
	"github.com/restic/chunker"

	"a4.io/blobstash/pkg/filetree/filetreeutil/meta"
	"a4.io/blobstash/pkg/hashutil"
)

var (
	pol = chunker.Pol(0x3c657535c4d6f5)
)

func (up *Uploader) writeReader(f io.Reader, meta *meta.Meta) error { // (*WriteResult, error) {
	// writeResult := NewWriteResult()
	// Init the rolling checksum

	// reuse this buffer
	buf := make([]byte, 8*1024*1024)
	// Prepare the reader to compute the hash on the fly
	fullHash := blake2b.New256()
	freader := io.TeeReader(f, fullHash)
	chunkSplitter := chunker.New(freader, pol)
	// TODO don't read one byte at a time if meta.Size < chunker.ChunkMinSize
	// Prepare the blob writer
	var size uint
	for {
		chunk, err := chunkSplitter.Next(buf)
		if err == io.EOF {
			break
		}
		chunkHash := hashutil.Compute(chunk.Data)
		size += chunk.Length

		exists, err := up.bs.Stat(chunkHash)
		if err != nil {
			panic(fmt.Sprintf("DB error: %v", err))
		}
		if !exists {
			if err := up.bs.Put(chunkHash, chunk.Data); err != nil {
				panic(fmt.Errorf("failed to PUT blob %v", err))
			}
		}

		// Save the location and the blob hash into a sorted list (with the offset as index)
		meta.AddIndexedRef(int(size), chunkHash)

		if err != nil {
			return err
		}
	}
	meta.Size = int(size)
	blake2bHash := fmt.Sprintf("%x", fullHash.Sum(nil))
	if meta.Data == nil {
		meta.Data = map[string]interface{}{
			"blake2b-hash": blake2bHash,
		}
	} else {
		meta.Data["blake2b-hash"] = blake2bHash
	}
	return nil
	// writeResult.Hash = fmt.Sprintf("%x", fullHash.Sum(nil))
	// if writeResult.BlobsUploaded > 0 {
	// 	writeResult.FilesCount++
	// 	writeResult.FilesUploaded++
	// }
	// return writeResult, nil
}

func (up *Uploader) PutFile(path string) (*meta.Meta, error) { // , *WriteResult, error) {
	up.StartUpload()
	defer up.UploadDone()
	fstat, err := os.Stat(path)
	if os.IsNotExist(err) {
		return nil, err
	}
	_, filename := filepath.Split(path)
	//sha, err := FullHash(path)
	//if err != nil {
	//	return nil, nil, fmt.Errorf("failed to compute fulle hash %v: %v", path, err)
	//}
	meta := meta.NewMeta()
	meta.Name = filename
	meta.Size = int(fstat.Size())
	meta.Type = "file"
	meta.ModTime = fstat.ModTime().Format(time.RFC3339)
	meta.Mode = uint32(fstat.Mode())
	// wr := NewWriteResult()
	if fstat.Size() > 0 {
		f, err := os.Open(path)
		defer f.Close()
		if err != nil {
			return nil, err
		}
		if err := up.writeReader(f, meta); err != nil {
			return nil, err
		}
		// if err != nil {
		// 	return nil, nil, fmt.Errorf("FileWriter error: %v", err)
		// }
		// wr.free()
		// wr = cwr
	}
	mhash, mjs := meta.Json()
	mexists, err := up.bs.Stat(mhash)
	if err != nil {
		return nil, fmt.Errorf("failed to stat blob %v: %v", mhash, err)
	}
	// wr.Size += len(mjs)
	if !mexists {
		if err := up.bs.Put(mhash, mjs); err != nil {
			return nil, fmt.Errorf("failed to put blob %v: %v", mhash, err)
		}
		// wr.BlobsCount++
		// wr.BlobsUploaded++
		// wr.SizeUploaded += len(mjs)
	} // else {
	// wr.SizeSkipped += len(mjs)
	// }
	meta.Hash = mhash
	return meta, nil
}

func (up *Uploader) PutMeta(meta *meta.Meta) error {
	mhash, mjs := meta.Json()
	mexists, err := up.bs.Stat(mhash)
	if err != nil {
		return fmt.Errorf("failed to stat blob %v: %v", mhash, err)
	}
	// wr.Size += len(mjs)
	if !mexists {
		if err := up.bs.Put(mhash, mjs); err != nil {
			return fmt.Errorf("failed to put blob %v: %v", mhash, err)
		}
		// wr.BlobsCount++
		// wr.BlobsUploaded++
		// wr.SizeUploaded += len(mjs)
	} // else {
	// wr.SizeSkipped += len(mjs)
	// }
	meta.Hash = mhash
	return nil
}

func (up *Uploader) RenameMeta(meta *meta.Meta, name string) error {
	meta.Name = filepath.Base(name)
	meta.ModTime = time.Now().Format(time.RFC3339)
	mhash, mjs := meta.Json()
	mexists, err := up.bs.Stat(mhash)
	if err != nil {
		return fmt.Errorf("failed to stat blob %v: %v", mhash, err)
	}
	// wr.Size += len(mjs)
	if !mexists {
		if err := up.bs.Put(mhash, mjs); err != nil {
			return fmt.Errorf("failed to put blob %v: %v", mhash, err)
		}
		// wr.BlobsCount++
		// wr.BlobsUploaded++
		// wr.SizeUploaded += len(mjs)
	} // else {
	// wr.SizeSkipped += len(mjs)
	// }
	meta.Hash = mhash
	return nil
}

// fmt.Sprintf("%x", blake2b.Sum256(js))
func (up *Uploader) PutReader(name string, reader io.ReadCloser, data map[string]interface{}) (*meta.Meta, error) { // *WriteResult, error) {
	up.StartUpload()
	defer up.UploadDone()

	meta := meta.NewMeta()
	meta.Name = filepath.Base(name)
	meta.Data = data
	meta.Type = "file"
	meta.ModTime = time.Now().Format(time.RFC3339)
	meta.Mode = uint32(0666)
	// wr := NewWriteResult()
	if err := up.writeReader(reader, meta); err != nil {
		return nil, err
	}
	// if err != nil {
	// 	return nil, nil, fmt.Errorf("FileWriter error: %v", err)
	// }
	// meta.Size = cwr.Size
	// wr.free()
	// wr = cwr
	mhash, mjs := meta.Json()
	mexists, err := up.bs.Stat(mhash)
	if err != nil {
		return nil, fmt.Errorf("failed to stat blob %v: %v", mhash, err)
	}
	// wr.Size += len(mjs)
	if !mexists {
		if err := up.bs.Put(mhash, mjs); err != nil {
			return nil, fmt.Errorf("failed to put blob %v: %v", mhash, err)
		}
		// wr.BlobsCount++
		// wr.BlobsUploaded++
		// wr.SizeUploaded += len(mjs)
	} // else {
	// wr.SizeSkipped += len(mjs)
	// }
	meta.Hash = mhash
	return meta, nil
}
