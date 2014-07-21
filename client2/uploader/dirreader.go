package uploader

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/garyburd/redigo/redis"
	"github.com/dchest/blake2b"

	"github.com/tsileo/blobstash/client2/ctx"
)

// DirIter returns a slice of the given directory children
func (up *Uploader) DirIter(con redis.Conn, ref string) ([]*Meta, error) {
	metas := []*Meta{}
	members, err := up.client.Smembers(con, ref)
	if err != nil {
		return nil, err
	}
	for _, metaRef := range members {
		meta := NewMeta()
		if err := up.client.HscanStruct(con, metaRef, meta); err != nil {
			return nil, err
		}
		metas = append(metas, meta)
	}
	return metas, nil
}

// GetDir restore the directory to path
func (up *Uploader) GetDir(cctx *ctx.Ctx, key, path string) (rr *ReadResult, err error) {
	fullHash := blake2b.New256()
	rr = &ReadResult{}
	err = os.Mkdir(path, os.ModeDir)
	if err != nil {
		return
	}
	con := up.client.ConnWithCtx(cctx)
	defer con.Close()
	meta := NewMeta()
	if err := up.client.HscanStruct(con, key, meta); err != nil {
		return nil, err
	}
	meta.Hash = key
	var crr *ReadResult
	if meta.Size > 0 {
		metas, err := up.DirIter(con, meta.Ref)
		if err != nil {
			return nil, fmt.Errorf("Error DirIter meta %+v: %v", meta, err)
		}
		for _, meta := range metas {
			if meta.IsFile() {
				crr, err = up.GetFile(cctx, meta.Hash, filepath.Join(path, meta.Name))
				if err != nil {
					return rr, err
				}
			} else {
				crr, err = up.GetDir(cctx, meta.Hash, filepath.Join(path, meta.Name))
				if err != nil {
					return rr, err
				}
			}
			fullHash.Write([]byte(crr.Hash))
			rr.Add(crr)
		}
	}
	// TODO(tsileo) sum the hash and check with the root
	rr.DirsCount++
	rr.DirsDownloaded++
	rr.Hash = fmt.Sprintf("%x", fullHash.Sum(nil))
	return
}
