package replication // import "a4.io/blobstash/pkg/replication"

import (
	"time"

	"a4.io/blobstash/pkg/client/oplog"
	"a4.io/blobstash/pkg/config"
	"a4.io/blobstash/pkg/sync"

	log "github.com/inconshreveable/log15"
)

type Replication struct {
	conf        *config.Config
	log         log.Logger
	synctable   *sync.Sync
	remoteOplog *oplog.Oplog
}

func New(logger log.Logger, conf *config.Config, s *sync.Sync) (*Replication, error) {
	logger.Debug("init")
	rep := &Replication{
		conf:        conf,
		log:         logger,
		remoteOplog: oplog.New(oplog.DefaultOpts().SetHost(conf.ReplicateTo.URL, conf.ReplicateTo.APIKey)),
		synctable:   s,
	}
	if err := rep.init(); err != nil {
		return nil, err
	}
	// FIXME(tsileo): clean shutdown
	return rep, nil
}

func (r *Replication) sync() error {
	stats, err := r.synctable.Sync(r.conf.ReplicateTo.URL, r.conf.ReplicateTo.APIKey)
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
				r.log.Info("new blob from replication", "hash", op.Data)
				// TODO(tsileo): save the blob locally from remote
			}
		}
	}()

	return nil
}
