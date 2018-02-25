package writer // import "a4.io/blobstash/pkg/filetree/writer"

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/dchest/blake2b"
	"github.com/restic/chunker"

	rnode "a4.io/blobstash/pkg/filetree/filetreeutil/node"
	"a4.io/blobstash/pkg/hashutil"
)

var (
	pol = chunker.Pol(0x3c657535c4d6f5)
)

func (up *Uploader) writeReader(f io.Reader, meta *rnode.RawNode) error { // (*WriteResult, error) {
	ctx := context.TODO()
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

		exists, err := up.bs.Stat(ctx, chunkHash)
		if err != nil {
			panic(fmt.Sprintf("DB error: %v", err))
		}
		if !exists {
			if err := up.bs.Put(ctx, chunkHash, chunk.Data); err != nil {
				panic(fmt.Errorf("failed to PUT blob %v", err))
			}
		}

		// Save the location and the blob hash into a sorted list (with the offset as index)
		meta.AddIndexedRef(int(size), chunkHash)
	}
	meta.Size = int(size)
	meta.AddData("blake2b-hash", fmt.Sprintf("%x", fullHash.Sum(nil)))
	return nil
	// writeResult.Hash = fmt.Sprintf("%x", fullHash.Sum(nil))
	// if writeResult.BlobsUploaded > 0 {
	// 	writeResult.FilesCount++
	// 	writeResult.FilesUploaded++
	// }
	// return writeResult, nil
}

func (up *Uploader) PutFileRename(path, filename string, extraMeta bool) (*rnode.RawNode, error) { // , *WriteResult, error) {
	return up.putFile(path, filename, extraMeta)
}

func (up *Uploader) PutFile(path string) (*rnode.RawNode, error) { // , *WriteResult, error) {
	_, filename := filepath.Split(path)
	return up.putFile(path, filename, true)
}

func (up *Uploader) putFile(path, filename string, extraMeta bool) (*rnode.RawNode, error) { // , *WriteResult, error) {
	ctx := context.TODO()
	up.StartUpload()
	defer up.UploadDone()
	fstat, err := os.Stat(path)
	if os.IsNotExist(err) {
		return nil, err
	}
	//sha, err := FullHash(path)
	//if err != nil {
	//	return nil, nil, fmt.Errorf("failed to compute fulle hash %v: %v", path, err)
	//}
	meta := &rnode.RawNode{}
	meta.Name = filename
	meta.Size = int(fstat.Size())
	meta.Type = "file"

	if extraMeta {
		// Only set the mode if it's not the default one
		mode := uint32(fstat.Mode())
		if mode != 0644 {
			meta.Mode = mode
		}

		// Mtime/Ctime handling
		meta.ModTime = fstat.ModTime().Unix()
		setMtime(meta, fstat)
		//if stat, ok := fstat.Sys().(*syscall.Stat_t); ok {
		//	meta.ChangeTime = stat.Ctim.Sec
		//}
	}

	// wr := NewWriteResult()
	if fstat.Size() > 0 {
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		if err := up.writeReader(f, meta); err != nil {
			return nil, err
		}
		// if err != nil {
		// 	return nil, nil, fmt.Errorf("FileWriter error: %v", err)
		// }
		// wr.free()
		// wr = cwr
	}
	mhash, mjs := meta.Encode()
	mexists, err := up.bs.Stat(ctx, mhash)
	if err != nil {
		return nil, fmt.Errorf("failed to stat blob %v: %v", mhash, err)
	}
	// wr.Size += len(mjs)
	if !mexists {
		if err := up.bs.Put(ctx, mhash, mjs); err != nil {
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

func (up *Uploader) PutMeta(meta *rnode.RawNode) error {
	ctx := context.TODO()
	mhash, mjs := meta.Encode()
	mexists, err := up.bs.Stat(ctx, mhash)
	if err != nil {
		return fmt.Errorf("failed to stat blob %v: %v", mhash, err)
	}
	// wr.Size += len(mjs)
	if !mexists {
		if err := up.bs.Put(ctx, mhash, mjs); err != nil {
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

func (up *Uploader) RenameMeta(meta *rnode.RawNode, name string) error {
	ctx := context.TODO()
	meta.Name = filepath.Base(name)
	mhash, mjs := meta.Encode()
	mexists, err := up.bs.Stat(ctx, mhash)
	if err != nil {
		return fmt.Errorf("failed to stat blob %v: %v", mhash, err)
	}
	// wr.Size += len(mjs)
	if !mexists {
		if err := up.bs.Put(ctx, mhash, mjs); err != nil {
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
func (up *Uploader) PutReader(name string, reader io.Reader, data map[string]interface{}) (*rnode.RawNode, error) { // *WriteResult, error) {
	ctx := context.TODO()
	up.StartUpload()
	defer up.UploadDone()

	meta := &rnode.RawNode{}
	meta.Name = filepath.Base(name)
	meta.Type = "file"
	meta.ModTime = time.Now().Unix()
	if data != nil {
		for k, v := range data {
			meta.AddData(k, v)
		}
	}
	// wr := NewWriteResult()
	if err := up.writeReader(reader, meta); err != nil {
		return nil, err
	}
	mhash, mjs := meta.Encode()
	mexists, err := up.bs.Stat(ctx, mhash)
	if err != nil {
		return nil, fmt.Errorf("failed to stat blob %v: %v", mhash, err)
	}
	// wr.Size += len(mjs)
	if !mexists {
		if err := up.bs.Put(ctx, mhash, mjs); err != nil {
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
