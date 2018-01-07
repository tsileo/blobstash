package blobstore // import "a4.io/blobstash/pkg/blobstore"

import (
	"context"
	"encoding/hex"
	"fmt"
	"path/filepath"

	log "github.com/inconshreveable/log15"

	"a4.io/blobsfile"

	// "a4.io/blobstash/pkg/backend/blobsfile"
	"a4.io/blobstash/pkg/backend/s3"
	"a4.io/blobstash/pkg/blob"
	"a4.io/blobstash/pkg/config"
	"a4.io/blobstash/pkg/hub"
)

var ErrBlobExists = fmt.Errorf("blob exist")

func NextHexKey(key string) string {
	bkey, err := hex.DecodeString(key)
	if err != nil {
		// XXX(tsileo): error invalid cursor?
		panic(err)
	}
	i := len(bkey)
	for i > 0 {
		i--
		bkey[i]++
		if bkey[i] != 0 {
			break
		}
	}
	return hex.EncodeToString(bkey)
}

type BlobStore struct {
	back   *blobsfile.BlobsFiles
	s3back *s3.S3Backend
	hub    *hub.Hub

	log log.Logger
}

func New(logger log.Logger, dir string, conf2 *config.Config, hub *hub.Hub) (*BlobStore, error) {
	logger.Debug("init")

	back, err := blobsfile.New(&blobsfile.Opts{Directory: filepath.Join(dir, "blobs")})
	if err != nil {
		return nil, fmt.Errorf("failed to init BlobsFile: %v", err)
	}
	var s3back *s3.S3Backend
	if conf2 != nil {
		if s3repl := conf2.S3Repl; s3repl != nil && s3repl.Bucket != "" {
			logger.Debug("init s3 replication")
			var err error
			s3back, err = s3.New(logger.New("app", "s3_replication"), back, hub, conf2)
			if err != nil {
				return nil, err
			}
		}
	}
	return &BlobStore{
		back:   back,
		s3back: s3back,
		hub:    hub,
		log:    logger,
	}, nil
}

func (bs *BlobStore) Close() error {
	// TODO(tsileo): improve this
	if bs.s3back != nil {
		bs.s3back.Close()
	}

	if err := bs.back.Close(); err != nil {
		return err
	}
	return nil
}

func (bs *BlobStore) Put(ctx context.Context, blob *blob.Blob) error {
	bs.log.Info("OP Put", "hash", blob.Hash, "len", len(blob.Data))

	// Ensure the blob hash match the blob content
	if err := blob.Check(); err != nil {
		return err
	}

	exists, err := bs.back.Exists(blob.Hash)
	if err != nil {
		return err
	}

	if exists {
		bs.log.Debug("blob already saved", "hash", blob.Hash)
		return nil
	}

	// Save the blob
	if err := bs.back.Put(blob.Hash, blob.Data); err != nil {
		return err
	}

	// Wait for adding the blob to the S3 replication queue if enabled
	if bs.s3back != nil {
		if err := bs.s3back.Put(blob.Hash); err != nil {
			return err
		}
	}

	// Wait for subscribed event completion
	if err := bs.hub.NewBlobEvent(ctx, blob, nil); err != nil {
		return err
	}

	bs.log.Debug("blob saved", "hash", blob.Hash)
	return nil
}

func (bs *BlobStore) GetEncoded(ctx context.Context, hash string) ([]byte, error) {
	bs.log.Info("OP Get (encoded)", "hash", hash)
	return bs.back.GetEncoded(hash)
}

func (bs *BlobStore) Get(ctx context.Context, hash string) ([]byte, error) {
	bs.log.Info("OP Get", "hash", hash)
	return bs.back.Get(hash)
}

func (bs *BlobStore) Stat(ctx context.Context, hash string) (bool, error) {
	bs.log.Info("OP Stat", "hash", hash)
	return bs.back.Exists(hash)
}

// func (backend *BlobsFileBackend) Enumerate(blobs chan<- *blob.SizedBlobRef, start, stop string, limit int) error {
func (bs *BlobStore) Enumerate(ctx context.Context, start, end string, limit int) ([]*blob.SizedBlobRef, string, error) {
	return bs.enumerate(ctx, start, end, limit, false)
}

func (bs *BlobStore) Scan(ctx context.Context) error {
	_, _, err := bs.enumerate(ctx, "", "\xff", 0, true)
	return err
}

func (bs *BlobStore) enumerate(ctx context.Context, start, end string, limit int, scan bool) ([]*blob.SizedBlobRef, string, error) {
	var cursor string
	bs.log.Info("OP Enumerate", "start", start, "end", end, "limit", limit)
	out := make(chan *blobsfile.Blob)
	refs := []*blob.SizedBlobRef{}
	errc := make(chan error, 1)
	go func() {
		errc <- bs.back.Enumerate(out, start, end, limit)
	}()
	for cblob := range out {
		if scan {
			fullblob, err := bs.Get(ctx, cblob.Hash)
			if err != nil {
				return nil, cursor, err
			}
			if err := bs.hub.ScanBlobEvent(ctx, &blob.Blob{Hash: cblob.Hash, Data: fullblob}, nil); err != nil {
				return nil, cursor, err
			}
		}
		refs = append(refs, &blob.SizedBlobRef{Hash: cblob.Hash, Size: cblob.Size})
	}
	if err := <-errc; err != nil {
		return nil, cursor, err
	}
	if len(refs) > 0 {
		cursor = NextHexKey(refs[len(refs)-1].Hash)
	}

	return refs, cursor, nil
}
