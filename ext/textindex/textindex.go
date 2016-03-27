package textindex

import (
	"github.com/gorilla/mux"
	log "gopkg.in/inconshreveable/log15.v2"

	serverMiddleware "github.com/tsileo/blobstash/middleware"
)

// TODO(tsileo): implement index with Bleve, think about `IndexBlob`, what to put in it (e.g. just the ref? additional fields in it?)
// FIXME(tsileo): once it's done, delete the DocStore Bleve index and provide hooks with this one

type TextIndexExt struct {
	log log.Logger
}

func New(log log.Logger) (*TextIndexExt, error) {
	return &TextIndexExt{
		log: log,
	}, nil
}

// RegisterRoute registers all the HTTP handlers for the extension
func (ti *TextIndexExt) RegisterRoute(root, r *mux.Router, middlewares *serverMiddleware.SharedMiddleware) {
	ti.log.Debug("RegisterRoute")
	// r.Handle("/node/{ref}", middlewares.Auth(http.HandlerFunc(ft.nodeHandler())))
}
