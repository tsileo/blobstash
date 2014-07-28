package clientutil

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/dchest/blake2b"
	"github.com/garyburd/redigo/redis"

	"github.com/tsileo/blobstash/client"
	"github.com/tsileo/blobstash/client/ctx"
)

// DirIter returns a slice of the given directory children
func DirIter(cl *client.Client, con redis.Conn, ref string) ([]*Meta, error) {
	metas := []*Meta{}
	members, err := cl.Smembers(con, ref)
	if err != nil {
		return nil, err
	}
	for _, metaRef := range members {
		meta := NewMeta()
		if err := cl.HscanStruct(con, metaRef, meta); err != nil {
			return nil, err
		}
		meta.Hash = metaRef
		metas = append(metas, meta)
	}
	return metas, nil
}

// GetDir restore the directory to path
func GetDir(cl *client.Client, cctx *ctx.Ctx, key, path string) (rr *ReadResult, err error) {
	fullHash := blake2b.New256()
	rr = &ReadResult{}
	err = os.Mkdir(path, 0700)
	if err != nil {
		return
	}
	con := cl.ConnWithCtx(cctx)
	defer con.Close()
	meta := NewMeta()
	if err := cl.HscanStruct(con, key, meta); err != nil {
		return nil, fmt.Errorf("failed to fetch meta: %v", err)
	}
	meta.Hash = key
	var crr *ReadResult
	if meta.Size > 0 {
		metas, err := DirIter(cl, con, meta.Ref)
		if err != nil {
			return nil, fmt.Errorf("Error DirIter meta %+v: %v", meta, err)
		}
		for _, meta := range metas {
			if meta.IsFile() {
				crr, err = GetFile(cl, cctx, meta.Hash, filepath.Join(path, meta.Name))
				if err != nil {
					return rr, fmt.Errorf("failed to GetFile %+v: %v", meta, err)
				}
			} else {
				crr, err = GetDir(cl, cctx, meta.Hash, filepath.Join(path, meta.Name))
				if err != nil {
					return rr, fmt.Errorf("failed to GetDir %+v: %v", meta, err)
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
