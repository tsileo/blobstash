package blobstore // import "a4.io/blobstash/pkg/blobstore"

import (
	"context"
	"encoding/hex"
	"expvar"
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

var (
	readVar  = expvar.NewInt("blobstore-read-bytes")
	writeVar = expvar.NewInt("blobstore-write-bytes")

	readCountVar  = expvar.NewInt("blobstore-read-count")
	writeCountVar = expvar.NewInt("blobstore-write-count")
)

var ErrBlobExists = fmt.Errorf("blob exist")

var ErrRemoteNotAvailable = fmt.Errorf("remote backend not available")

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

	hub  *hub.Hub
	root bool
	stop chan struct{}

	log log.Logger
}

func New(logger log.Logger, root bool, dir string, conf2 *config.Config, hub *hub.Hub) (*BlobStore, error) {
	logger.Debug("init")
	back, err := blobsfile.New(&blobsfile.Opts{
		Compression: blobsfile.Snappy,
		Directory:   filepath.Join(dir, "blobs"),
		LogFunc: func(msg string) {
			logger.Info(msg, "submodule", "blobsfile")
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to init BlobsFile: %v", err)
	}
	var s3back *s3.S3Backend
	if root && conf2 != nil {
		if s3repl := conf2.S3Repl; s3repl != nil && s3repl.Bucket != "" {
			logger.Debug("init s3 replication")
			var err error
			s3back, err = s3.New(logger.New("app", "s3_replication"), back, hub, conf2, filepath.Join(dir, "blobs"))
			if err != nil {
				return nil, err
			}
		}
	}
	bs := &BlobStore{
		back:   back,
		root:   root,
		s3back: s3back,
		hub:    hub,
		log:    logger,
		stop:   make(chan struct{}),
	}

	if bs.root && bs.s3back != nil {
		bs.back.SetBlobsFilesSealedFunc(func(path string) {
			go func(path string) {
				if err := bs.s3back.BlobsFilesUploadPack(path); err != nil {
					logger.Error("failed to upload pack", "path", path, "err", err)
				}
			}(path)
		})
		go func() {
			if err := bs.s3back.BlobsFilesSyncWorker(bs.back.SealedPacks()); err != nil {
				logger.Error("failed to sync BlobsFile", "err", err)
			}
		}()
	}

	return bs, nil
}

func (bs *BlobStore) Check() error {
	if err := bs.back.CheckBlobsFiles(); err != nil {
		return err
	}

	return nil
}

func (bs *BlobStore) ReplicationEnabled() bool {
	return bs.s3back != nil
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

func (bs *BlobStore) S3Stats() (map[string]interface{}, error) {
	fmt.Printf("S3Stats %+v %+v\n\n", bs.root, bs.s3back)
	if !bs.root || bs.s3back == nil {
		return nil, ErrRemoteNotAvailable
	}
	return bs.s3back.Stats()
}

func (bs *BlobStore) Put(ctx context.Context, blob *blob.Blob) (bool, error) {
	bs.log.Info("OP Put", "hash", blob.Hash, "len", len(blob.Data))
	var saved bool

	// Ensure the blob hash match the blob content
	if err := blob.Check(); err != nil {
		return saved, err
	}

	exists, err := bs.back.Exists(blob.Hash)
	if err != nil {
		return saved, err
	}

	if exists {
		bs.log.Debug("blob already saved", "hash", blob.Hash)
		return saved, nil
	}

	saved = true

	var specialBlob bool
	if blob.IsMeta() || blob.IsFiletreeNode() {
		specialBlob = true
	}

	// Save the blob
	if err := bs.back.Put(blob.Hash, blob.Data); err != nil {
		return saved, err
	}

	// Wait for adding the blob to the S3 replication queue if enabled
	if bs.root && bs.s3back != nil {
		if err := bs.s3back.Put(blob.Hash); err != nil {
			return saved, err
		}
	}

	// Wait for subscribed event completion
	if err := bs.hub.NewBlobEvent(ctx, blob, nil); err != nil {
		return saved, err
	}

	writeCountVar.Add(1)
	writeVar.Add(int64(len(blob.Data)))

	bs.log.Debug("blob saved", "hash", blob.Hash, "special_blob", specialBlob)
	return saved, nil
}

func (bs *BlobStore) Stats() (*blobsfile.Stats, error) {
	return bs.back.Stats()
}

func (bs *BlobStore) Get(ctx context.Context, hash string) ([]byte, error) {
	bs.log.Info("OP Get", "hash", hash)
	blob, err := bs.back.Get(hash)
	if err != nil {
		return nil, err
	}

	readCountVar.Add(1)
	readVar.Add(int64(len(blob)))

	return blob, err
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
		if start == "" && end == "\xff" || end == "" {
			errc <- bs.back.EnumeratePrefix(out, start, limit)

		} else {
			errc <- bs.back.Enumerate(out, start, end, limit)

		}
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
