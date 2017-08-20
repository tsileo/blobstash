package replication // import "a4.io/blobstash/pkg/replication"

import (
	"context"
	"math"
	"sync"
	"time"

	"a4.io/blobstash/pkg/blob"
	"a4.io/blobstash/pkg/client/oplog"
	"a4.io/blobstash/pkg/config"
	"a4.io/blobstash/pkg/stash/store"
	bsync "a4.io/blobstash/pkg/sync"

	log "github.com/inconshreveable/log15"
)

type Backoff struct {
	delay    time.Duration
	factor   float64
	maxDelay time.Duration
	attempt  int
}

func (b *Backoff) Reset() {
	b.attempt = 1
}

func (b *Backoff) Delay() time.Duration {
	d := float64(b.delay) * math.Pow(b.factor, float64(b.attempt))
	maxD := float64(b.maxDelay)
	b.attempt++
	if d > maxD {
		return time.Duration(maxD)
	}
	return time.Duration(d)
}

type Replication struct {
	log       log.Logger
	synctable *bsync.Sync
	blobstore store.BlobStore
	backoff   *Backoff

	remoteOplog *oplog.Oplog

	conf *config.ReplicateFrom

	wg sync.WaitGroup
}

func New(logger log.Logger, conf *config.Config, bs store.BlobStore, s *bsync.Sync, wg sync.WaitGroup) (*Replication, error) {
	logger.Debug("init")
	rep := &Replication{
		conf:        conf.ReplicateFrom,
		blobstore:   bs,
		log:         logger,
		remoteOplog: oplog.New(oplog.DefaultOpts().SetHost(conf.ReplicateFrom.URL, conf.ReplicateFrom.APIKey)),
		synctable:   s,
		backoff: &Backoff{
			delay:    1 * time.Second,
			maxDelay: 120 * time.Second,
			factor:   1.6,
		},
		wg: wg,
	}
	if err := rep.init(); err != nil {
		return nil, err
	}
	// FIXME(tsileo): clean shutdown
	return rep, nil
}

func (r *Replication) sync() error {
	stats, err := r.synctable.Sync(r.conf.URL, r.conf.APIKey)
	if err != nil {
		return err
	}
	r.log.Info("sync done", "stats", stats)
	return nil
}

func (r *Replication) init() error {
	r.log.Debug("initial sync")
	r.backoff.Reset()
	if err := r.sync(); err != nil {
		return err
	}
	var resync bool

	ops := make(chan *oplog.Op)

	// This should run forever (can't disable replication while BlobStash is already running)
	go func() {
		for {
			if resync {
				r.log.Debug("trying to resync")
				if err := r.sync(); err != nil {
					r.log.Error("failed to sync", "err", err, "attempt", r.backoff.attempt)
					time.Sleep(r.backoff.Delay())
					continue
				}
				r.backoff.Reset()
				r.log.Debug("sync successful")
				resync = false
			}

			r.log.Debug("listen to remote oplog")
			if err := r.remoteOplog.Notify(ops); err != nil {
				r.log.Error("remote oplog SSE error", "err", err, "attempt", r.backoff.attempt)
				resync = true
				time.Sleep(r.backoff.Delay())
			}
			r.backoff.Reset()
		}
	}()

	go func() {
		for op := range ops {
			if op.Event == "blob" {
				hash := op.Data
				r.log.Info("new blob from replication", "hash", hash)

				// Fetch the blob from the remote BlobStash instance
				data, err := r.remoteOplog.GetBlob(hash)
				if err != nil {
					panic(err)
				}

				// Ensure the blob is not corrupted
				blob := &blob.Blob{Hash: hash, Data: data}
				r.log.Debug("fetched blob", "blob", blob)

				// Save it locally
				if r.blobstore.Put(context.Background(), blob); err != nil {
					panic(err)
				}
			}
		}
		r.log.Debug("done listening the remote oplog")
	}()

	return nil
}
