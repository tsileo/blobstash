package server

import (
	"fmt"
	"net/http"
	"os"

	"github.com/tsileo/blobstash/pkg/blobstore"
	"github.com/tsileo/blobstash/pkg/kvstore"

	"github.com/gorilla/mux"
	log "github.com/inconshreveable/log15"
)

type App interface {
	Register(*mux.Router)
}

type Server struct {
	router *mux.Router
}

func New() (*Server, error) {
	s := &Server{
		router: mux.NewRouter(),
	}
	logger := log.New()
	logger.SetHandler(log.StreamHandler(os.Stdout, log.TerminalFormat()))
	// Load the blobstore
	blobstore, err := blobstore.New(logger.New("app", "blobstore"))
	if err != nil {
		return nil, fmt.Errorf("failed to initialize blobstore app")
	}
	// FIXME(tsileo): handle middleware in the `Register` interface
	blobstore.Register(s.router.PathPrefix("/api/blobstore").Subrouter())
	// Load the kvstore
	kvstore, err := kvstore.New(logger.New("app", "kvstore"), blobstore)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize kvstore app")
	}
	kvstore.Register(s.router.PathPrefix("/api/kvstore").Subrouter())
	return s, nil
}

func (s *Server) Serve() error {
	return http.ListenAndServe(":8051", s.router)
}
