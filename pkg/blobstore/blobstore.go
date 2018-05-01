package blobstore // import "a4.io/blobstash/pkg/blobstore"

import (
	"context"
	"encoding/hex"
	"expvar"
	"fmt"
	"path/filepath"

	"github.com/golang/snappy"
	log "github.com/inconshreveable/log15"

	"a4.io/blobsfile"

	// "a4.io/blobstash/pkg/backend/blobsfile"
	"a4.io/blobstash/pkg/backend/s3"
	"a4.io/blobstash/pkg/blob"
	"a4.io/blobstash/pkg/cache"
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
	back      *blobsfile.BlobsFiles
	s3back    *s3.S3Backend
	dataCache *cache.Cache

	hub  *hub.Hub
	root bool

	log log.Logger
}

func New(logger log.Logger, root bool, dir string, conf2 *config.Config, hub *hub.Hub) (*BlobStore, error) {
	logger.Debug("init")

	back, err := blobsfile.New(&blobsfile.Opts{Directory: filepath.Join(dir, "blobs")})
	if err != nil {
		return nil, fmt.Errorf("failed to init BlobsFile: %v", err)
	}
	var dataCache *cache.Cache
	var s3back *s3.S3Backend
	if root && conf2 != nil {
		dataCache, err = cache.New(conf2.VarDir(), "data_blobs.cache", 50*1024<<20) // 50GB cache
		if err != nil {
			return nil, err
		}
		if s3repl := conf2.S3Repl; s3repl != nil && s3repl.Bucket != "" {
			logger.Debug("init s3 replication")
			var err error
			s3back, err = s3.New(logger.New("app", "s3_replication"), back, dataCache, hub, conf2)
			if err != nil {
				return nil, err
			}
		}
	}
	return &BlobStore{
		back:      back,
		root:      root,
		s3back:    s3back,
		dataCache: dataCache,
		hub:       hub,
		log:       logger,
	}, nil
}

func (bs *BlobStore) Close() error {
	// TODO(tsileo): improve this
	if bs.dataCache != nil {
		bs.dataCache.Close()
	}
	if bs.s3back != nil {
		bs.s3back.Close()
	}

	if err := bs.back.Close(); err != nil {
		return err
	}
	return nil
}

func (bs *BlobStore) GetRemoteRef(ref string) (string, error) {
	fmt.Printf("GetRemoteRef %s %+v %+v\n\n", ref, bs.root, bs.s3back)
	if !bs.root || bs.s3back == nil {
		return "", ErrRemoteNotAvailable
	}
	return bs.s3back.GetRemoteRef(ref)
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

	var specialBlob bool
	if blob.IsMeta() || blob.IsFiletreeNode() {
		specialBlob = true
	}

	if !bs.root || bs.root && specialBlob || bs.root && bs.s3back == nil {
		// Save the blob
		if err := bs.back.Put(blob.Hash, blob.Data); err != nil {
			return err
		}
	} else {
		bs.log.Info("saving the blob in the data cache", "hash", blob.Hash)
		exists, err := bs.dataCache.Stat(blob.Hash)
		if err != nil {
			return err
		}
		if !exists {
			// This is most likely a data blob and the blob will be stored elsewhere
			if err := bs.dataCache.Add(blob.Hash, blob.Data); err != nil {
				return err
			}
		}
	}

	// Wait for adding the blob to the S3 replication queue if enabled
	if bs.root && bs.s3back != nil {
		if err := bs.s3back.Put(blob.Hash); err != nil {
			return err
		}
	}

	// Wait for subscribed event completion
	if err := bs.hub.NewBlobEvent(ctx, blob, nil); err != nil {
		return err
	}

	writeCountVar.Add(1)
	writeVar.Add(int64(len(blob.Data)))

	bs.log.Debug("blob saved", "hash", blob.Hash)
	return nil
}

func (bs *BlobStore) GetEncoded(ctx context.Context, hash string) ([]byte, error) {
	bs.log.Info("OP Get (encoded)", "hash", hash)
	blob, err := bs.back.GetEncoded(hash)
	switch err {
	case nil:
	case blobsfile.ErrBlobNotFound:
		if !bs.root || bs.s3back == nil {
			return nil, err
		}

		// The blob may be queued for download
		inFlight, blb, berr := bs.s3back.BlobWaitingForDownload(hash)
		if berr != nil {
			return nil, berr
		}
		if inFlight {
			// The blob is queued for download, force the download
			blob = blb.Data
			blob = snappy.Encode(nil, blb.Data)
			err = nil
			break
		}

		// If there's a data cache, it means the blob must be stored on S3
		if bs.dataCache != nil {
			cached, err := bs.dataCache.Stat(hash)
			if err != nil {
				return nil, err
			}
			if cached {
				bs.log.Debug("blob found in the data cache", "hash", hash)
				blob, _, err = bs.dataCache.Get(hash)
				blob = snappy.Encode(nil, blb.Data)
				break
			}

			// If the blob is available on S3, download it and add it to the data cache
			indexed, err := bs.s3back.Indexed(hash)
			if err != nil {
				return nil, err
			}

			if indexed {
				bs.log.Debug("blob found on S3", "hash", hash)
				blob, err = bs.s3back.Get(hash)
				if err != nil {
					return nil, err
				}
				if err := bs.dataCache.Add(hash, blob); err != nil {
					return nil, err
				}
				blob = snappy.Encode(nil, blb.Data)
				break
			}
		}

		// Return the original error
		return nil, err

		err = nil
	default:
		return nil, err
	}

	readCountVar.Add(1)
	readVar.Add(int64(len(blob)))

	return blob, err
}

func (bs *BlobStore) Get(ctx context.Context, hash string) ([]byte, error) {
	bs.log.Info("OP Get", "hash", hash)
	blob, err := bs.back.Get(hash)
	switch err {
	case nil:
	case blobsfile.ErrBlobNotFound:
		if !bs.root || bs.s3back == nil {
			return nil, err
		}

		// The blob may be queued for download
		inFlight, blb, berr := bs.s3back.BlobWaitingForDownload(hash)
		if berr != nil {
			return nil, berr
		}
		if inFlight {
			// The blob is queued for download, force the download
			blob = blb.Data
			err = nil
			break
		}

		// If there's a data cache, it means the blob must be stored on S3
		if bs.dataCache != nil {
			cached, err := bs.dataCache.Stat(hash)
			if err != nil {
				return nil, err
			}
			if cached {
				bs.log.Debug("blob found in the data cache", "hash", hash)
				blob, _, err = bs.dataCache.Get(hash)
				break
			}

			// If the blob is available on S3, download it and add it to the data cache
			indexed, err := bs.s3back.Indexed(hash)
			if err != nil {
				return nil, err
			}

			if indexed {
				bs.log.Debug("blob found on S3", "hash", hash)
				blob, err = bs.s3back.Get(hash)
				if err != nil {
					return nil, err
				}
				if err := bs.dataCache.Add(hash, blob); err != nil {
					return nil, err
				}
				break
			}
		}

		// Return the original error
		return nil, err
	default:
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
