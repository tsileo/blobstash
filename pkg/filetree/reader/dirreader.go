package reader

import (
	"fmt"
	"os"
	"path/filepath"

	_ "github.com/dchest/blake2b"
	"golang.org/x/net/context"

	"github.com/tsileo/blobstash/pkg/client/blobstore"
	"github.com/tsileo/blobstash/pkg/filetree/filetreeutil/meta"
	"github.com/tsileo/blobstash/pkg/filetree/reader/filereader"
)

// GetDir restore the directory to path
func GetDir(bs *blobstore.BlobStore, hash, path string) error { // (rr *ReadResult, err error) {
	// fullHash := blake2b.New256()
	// rr = &ReadResult{}
	if err := os.Mkdir(path, 0700); err != nil {
		return err
	}

	js, err := bs.Get(context.TODO(), hash)
	if err != nil {
		return err
	}
	cmeta, err := meta.NewMetaFromBlob(hash, js)
	if err != nil {
		return fmt.Errorf("failed to fetch meta %s: %v", hash, err)
	}
	cmeta.Hash = hash
	// var crr *ReadResult
	if cmeta.Size > 0 {
		for _, hash := range cmeta.Refs {
			blob, err := bs.Get(context.TODO(), hash.(string))
			submeta, err := meta.NewMetaFromBlob(hash.(string), blob)
			if err != nil {
				return fmt.Errorf("failed to fetch meta: %v", err)
			}
			if submeta.IsFile() {
				if err := filereader.GetFile(bs, submeta.Hash, filepath.Join(path, submeta.Name)); err != nil {
					return fmt.Errorf("failed to GetFile %+v: %v", submeta, err)
				}
			} else {
				if err := GetDir(bs, submeta.Hash, filepath.Join(path, submeta.Name)); err != nil {
					return fmt.Errorf("failed to GetDir %+v: %v", submeta, err)
				}
			}
			// fullHash.Write([]byte(crr.Hash))
			// rr.Add(crr)
		}
	}
	// TODO(tsileo): sum the hash and check with the root
	// rr.DirsCount++
	// rr.DirsDownloaded++
	// rr.Hash = fmt.Sprintf("%x", fullHash.Sum(nil))
	return nil
}
