/*

http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html#cli-config-files


*/
package s3 // import "a4.io/blobstash/pkg/backend/s3"

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	humanize "github.com/dustin/go-humanize"
	log "github.com/inconshreveable/log15"

	"a4.io/blobsfile"

	"a4.io/blobstash/pkg/backend/s3/index"
	"a4.io/blobstash/pkg/backend/s3/s3util"
	"a4.io/blobstash/pkg/blob"
	"a4.io/blobstash/pkg/config"
	"a4.io/blobstash/pkg/docstore/id"
	"a4.io/blobstash/pkg/hashutil"
	"a4.io/blobstash/pkg/hub"
	"a4.io/blobstash/pkg/queue"
)

// TODO(tsileo):
// - HTTP endpoint to trigger the SyncRemoteBlob event
// - make the stash/data ctx handle the remote blobs by sending
// - TODO use data ctx name for the tmp/ -> stash/{stash-name}, (have the GC scan all the encrypted blobs and builds an in-memory index OR have the client send its index -> faster), do the GC, send the SyncRemoteBlob from within the GC (that should be async)
// and have BlobStash stream from S3 in the meantime (and blocking the download queue to be sure we don't download blob twice)???
// - make the FS use iputil, -auto-remote-blobs and -force-remote-blobs flags
// - Make the upload optionally remote??
// - Have a stash unique ID to protect the remote S3 local index kv
// - SyncRemoteBlob should copy the blob after the copy and the GC delete the rest

var ErrWriteOnly = errors.New("backend is in read-only mode")

type dlIndexItem struct {
	blob *blob.Blob
	id   *id.ID
}

type S3Backend struct {
	log log.Logger

	uploadQueue *queue.Queue
	index       *index.Index

	encrypted bool
	key       *[32]byte

	backend *blobsfile.BlobsFiles
	hub     *hub.Hub

	wg sync.WaitGroup

	s3   *s3.S3
	stop chan struct{}

	bucket string
}

func New(logger log.Logger, back *blobsfile.BlobsFiles, h *hub.Hub, conf *config.Config) (*S3Backend, error) {
	// Parse config
	bucket := conf.S3Repl.Bucket
	region := conf.S3Repl.Region
	scanMode := conf.S3ScanMode
	restoreMode := conf.S3RestoreMode
	key, err := conf.S3Repl.Key()
	if err != nil {
		return nil, err
	}

	var s3svc *s3.S3
	if conf.S3Repl.Endpoint != "" {
		s3svc, err = s3util.NewWithCustomEndoint(conf.S3Repl.AccessKey, conf.S3Repl.SecretKey, region, conf.S3Repl.Endpoint)
		if err != nil {
			return nil, err
		}
	} else {
		// Create a S3 Session
		s3svc, err = s3util.New(region)
		if err != nil {
			return nil, err
		}
	}
	// Init the disk-backed queue
	uq, err := queue.New(filepath.Join(conf.VarDir(), "s3-upload.queue"))
	if err != nil {
		return nil, err
	}

	// Init the disk-backed index
	indexPath := filepath.Join(conf.VarDir(), "s3-backend.index")
	if scanMode || restoreMode {
		logger.Debug("trying to remove old index file")
		os.Remove(indexPath)
	}
	i, err := index.New(indexPath)
	if err != nil {
		return nil, err
	}

	s3backend := &S3Backend{
		log:         logger,
		backend:     back,
		hub:         h,
		s3:          s3svc,
		stop:        make(chan struct{}),
		bucket:      bucket,
		key:         key,
		uploadQueue: uq,
		index:       i,
	}

	// FIXME(tsileo): should encypption be optional?
	if key != nil {
		s3backend.encrypted = true
	}

	logger.Info("Initializing S3 replication", "bucket", bucket, "encrypted", s3backend.encrypted, "scan_mode", scanMode, "restore_mode", restoreMode)

	// Ensure the bucket exist
	obucket := s3util.NewBucket(s3backend.s3, bucket)
	ok, err := obucket.Exists()
	if err != nil {
		return nil, err
	}

	// Create it if it does not
	if !ok {
		logger.Info("creating bucket", "bucket", bucket)
		if err := obucket.Create(); err != nil {
			return nil, err
		}
	}

	// Trigger a re-indexing/full restore if requested
	if scanMode || restoreMode {
		if err := s3backend.reindex(obucket, restoreMode); err != nil {
			return nil, err
		}
	}

	// Initialize the worker (queue consumer)
	go s3backend.uploadWorker()

	return s3backend, nil
}

func (b *S3Backend) String() string {
	suf := ""
	if b.encrypted {
		suf = "-encrypted"
	}
	return fmt.Sprintf("s3-backend-%s", b.bucket) + suf
}

func (b *S3Backend) Stats() (map[string]interface{}, error) {
	total := 0
	count := 0

	blbs, err := b.uploadQueue.Blobs()
	if err != nil {
		return nil, fmt.Errorf("failed to get blobs from queue: %v", err)
	}

	for _, blb := range blbs {
		count += 1
		sz, err := b.backend.Size(blb.Hash)
		if err != nil {
			return nil, err
		}
		total += sz
	}

	return map[string]interface{}{"blobs_waiting": count, "blobs_size": total}, nil
}

func (b *S3Backend) Put(hash string) error {
	if _, err := b.uploadQueue.Enqueue(&blob.Blob{Hash: hash}); err != nil {
		return err
	}
	return nil
}

func (b *S3Backend) reindex(bucket *s3util.Bucket, restore bool) error {
	b.log.Info("Starting S3 re-indexing")
	start := time.Now()
	max := 100
	cnt := 0

	if err := bucket.Iter(max, func(object *s3util.Object) error {
		b.log.Debug("fetching an objects batch from S3")
		ehash := object.Key
		eblob := s3util.NewEncryptedBlob(object, b.key)
		hash, err := eblob.PlainTextHash()
		if err != nil {
			return err
		}
		b.log.Debug("indexing plain-text hash", "hash", hash)

		if err := b.index.Index(hash, ehash); err != nil {
			return err
		}

		if restore {
			// Here we interact with the BlobsFile directly, which is quite dangerous
			// (the hub event is crucial here to behave like the BlobStore)

			exists, err := b.backend.Exists(hash)
			if err != nil {
				return err
			}

			if exists {
				b.log.Debug("blob already saved", "hash", hash)
				return nil
			}

			// FIXME(tsileo): check if the blob is a "data blob" thanks to the new flag and skip the blob if needed

			data, err := eblob.PlainText()
			if err != nil {
				return err
			}

			if err := b.backend.Put(hash, data); err != nil {
				return err
			}

			// Wait for subscribed event completion
			if err := b.hub.NewBlobEvent(context.TODO(), &blob.Blob{
				Hash: hash,
				Data: data,
			}, nil); err != nil {
				return err
			}

		}

		return nil
	}); err != nil {
		return err
	}

	b.log.Info("S3 scan done", "objects_downloaded_cnt", cnt, "duration", time.Since(start))
	start = time.Now()
	cnt = 0
	out := make(chan *blobsfile.Blob)
	errc := make(chan error, 1)
	go func() {
		errc <- b.backend.Enumerate(out, "", "\xff", 0)
	}()
	for blob := range out {
		exists, err := b.index.Exists(blob.Hash)
		if err != nil {
			return err
		}
		if !exists {
			t := time.Now()
			b.wg.Add(1)
			defer b.wg.Done()
			data, err := b.backend.Get(blob.Hash)
			if err != nil {
				return err
			}
			if err := b.put(blob.Hash, data); err != nil {
				return err
			}
			cnt++
			b.log.Info("blob uploaded to s3", "hash", blob.Hash, "duration", time.Since(t))
		}
	}
	if err := <-errc; err != nil {
		return err
	}
	b.log.Info("local scan done", "objects_uploaded_cnt", cnt, "duration", time.Since(start))
	return nil
}

func (b *S3Backend) uploadWorker() {
	log := b.log.New("worker", "upload_worker")
	log.Debug("starting worker")
	total := uint64(0)
L:
	for {
		select {
		case <-b.stop:
			log.Debug("worker stopped")
			break L
		default:
			// log.Debug("polling")
			blb := &blob.Blob{}
			ok, deqFunc, err := b.uploadQueue.Dequeue(blb)
			if err != nil {
				panic(err)
			}
			if ok {
				if err := func(blob *blob.Blob) error {
					t := time.Now()
					b.wg.Add(1)
					defer b.wg.Done()
					data, err := b.backend.Get(blob.Hash)
					if err != nil {
						deqFunc(false)
						return err
					}
					// Double check the blob does not exists
					exists, err := b.index.Exists(blob.Hash)
					if err != nil {
						deqFunc(false)
						return err
					}
					if exists {
						log.Debug("blob already exist", "hash", blob.Hash)
						deqFunc(true)
						return nil
					}

					if err := b.put(blob.Hash, data); err != nil {
						deqFunc(false)
						return err
					}
					deqFunc(true)
					blobSize := uint64(len(data))
					total += blobSize
					log.Info("blob uploaded to s3", "hash", blob.Hash, "size", humanize.Bytes(blobSize), "duration", time.Since(t), "uploaded_since_startup", humanize.Bytes(total))

					return nil
				}(blb); err != nil {
					log.Error("failed to upload blob", "hash", blb.Hash, "err", err)
					time.Sleep(1 * time.Second)
				}
				continue L
			}
			time.Sleep(1 * time.Second)
			continue L
		}
	}
}

func (b *S3Backend) put(hash string, data []byte) error {
	// At this point, we're sure the blob does not exist remotely

	ehash := hash
	// Encrypt if requested
	if b.encrypted {
		var err error
		data, err = s3util.Seal(b.key, &blob.Blob{Hash: hash, Data: data})
		if err != nil {
			return err
		}
		// Re-compute the hash
		ehash = hashutil.Compute(data)
	}

	// Prepare the upload request
	params := &s3.PutObjectInput{
		Bucket:   aws.String(b.bucket),
		Key:      aws.String(ehash),
		Body:     bytes.NewReader(data),
		Metadata: map[string]*string{},
	}

	// Actually upload the blob
	if _, err := b.s3.PutObject(params); err != nil {
		return err
	}

	// Save the hash in the local index
	if err := b.index.Index(hash, ehash); err != nil {
		return nil
	}

	return nil
}

func (b *S3Backend) Indexed(hash string) (bool, error) {
	return b.index.Exists(hash)
}

func (b *S3Backend) Exists(hash string) (bool, error) {
	return b.Indexed(hash)
}

func (b *S3Backend) Get(hash string) ([]byte, error) {
	ehash, err := b.index.Get(hash)
	if err != nil {
		return nil, err
	}

	obj, err := s3util.NewBucket(b.s3, b.bucket).GetObject(ehash)
	if err != nil {
		return nil, err
	}
	eblob := s3util.NewEncryptedBlob(obj, b.key)
	fhash, data, err := eblob.HashAndPlainText()
	if fhash != hash {
		return nil, fmt.Errorf("hash does not match")
	}

	return data, err
}

func (b *S3Backend) GetRemoteRef(pref string) (string, error) {
	return b.index.Get(pref)
}

func (b *S3Backend) Close() {
	b.log.Debug("stopping workers")
	b.stop <- struct{}{}
	b.stop <- struct{}{}
	b.stop <- struct{}{}
	b.log.Debug("waiting for waitgroup")
	b.wg.Wait()
	b.log.Debug("done")
	b.uploadQueue.Close()
	b.log.Debug("queues closed")
	b.index.Close()
	b.log.Debug("s3 backend closed")
}
