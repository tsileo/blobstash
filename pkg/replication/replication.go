package replication // import "a4.io/blobstash/pkg/replication"

import (
	"context"
	"time"

	"a4.io/blobstash/pkg/blob"
	"a4.io/blobstash/pkg/blobstore"
	"a4.io/blobstash/pkg/client/oplog"
	"a4.io/blobstash/pkg/config"
	"a4.io/blobstash/pkg/sync"

	log "github.com/inconshreveable/log15"
)

type Replication struct {
	log       log.Logger
	synctable *sync.Sync
	blobstore *blobstore.BlobStore

	remoteOplog *oplog.Oplog

	conf *config.ReplicateFrom
}

func New(logger log.Logger, conf *config.Config, s *sync.Sync) (*Replication, error) {
	logger.Debug("init")
	rep := &Replication{
		conf:        conf.ReplicateFrom,
		log:         logger,
		remoteOplog: oplog.New(oplog.DefaultOpts().SetHost(conf.ReplicateFrom.URL, conf.ReplicateFrom.APIKey)),
		synctable:   s,
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
	if err := r.sync(); err != nil {
		return err
	}
	var resync bool

	ops := make(chan *oplog.Op)

	go func() {
		for {
			if resync {
				r.log.Debug("trying to resync")
				if err := r.sync(); err != nil {
					r.log.Error("failed to sync", "err", err)
					continue
				}
				r.log.Debug("sync successful")
				resync = false
			}

			r.log.Debug("listen to remote oplog")
			if err := r.remoteOplog.Notify(ops); err != nil {
				r.log.Error("remote oplog SSE error", "err", err)
				resync = true
				time.Sleep(5 * time.Second)
			}
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
				blob := &blob.Blob{Hash: op.Data, Data: data}
				if err := blob.Check(); err != nil {
					panic(err)
				}

				// Save it locally
				if r.blobstore.Put(context.Background(), blob); err != nil {
					panic(err)
				}
			}
		}
	}()

	return nil
}
