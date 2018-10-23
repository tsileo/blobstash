package capabilities // import "a4.io/blobstash/pkg/capabilities"

import (
	"net/http"

	"a4.io/blobstash/pkg/blobstore"
	"a4.io/blobstash/pkg/config"
	"a4.io/blobstash/pkg/httputil"
	"a4.io/blobstash/pkg/hub"

	"github.com/gorilla/mux"
	log "github.com/inconshreveable/log15"
)

type Capabilities struct {
	bs   *blobstore.BlobStore
	hub  *hub.Hub
	log  log.Logger
	conf *config.Config
}

func New(logger log.Logger, conf *config.Config, bs *blobstore.BlobStore, h *hub.Hub) (*Capabilities, error) {
	logger.Debug("init")
	capa := &Capabilities{
		log:  logger,
		conf: conf,
		bs:   bs,
		hub:  h,
	}
	return capa, nil
}

func (c *Capabilities) Register(r *mux.Router, basicAuth func(http.Handler) http.Handler) {
	// Register the SSE HTTP endpoint
	r.Handle("/", basicAuth(http.HandlerFunc(c.indexHandler)))
}

func (c *Capabilities) indexHandler(w http.ResponseWriter, r *http.Request) {
	httputil.MarshalAndWrite(r, w, map[string]interface{}{
		"data": map[string]interface{}{
			"replication_enabled": c.bs.ReplicationEnabled(),
			// TODO(tsileo): flag for the oplog
		},
	})
}
