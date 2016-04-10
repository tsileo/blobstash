package server

import (
	"fmt"
	"net/http"

	"github.com/tsileo/blobstash/pkg/blobstore"

	"github.com/gorilla/mux"
)

type Apper interface {
	// Register(*mux.Router)
	Name() string
	Hook() interface{}
	Required() []string
	AddHook(string, interface{})
}

type Server struct {
	apps   map[string]Apper
	router *mux.Router
}

func New() (*Server, error) {
	s := &Server{
		apps:   map[string]Apper{},
		router: mux.NewRouter(),
	}
	// Routes consist of a path and a handler function.

	// Load the blobstore
	blobstore, err := blobstore.New()
	if err != nil {
		return nil, fmt.Errorf("failed to initialize blobstore app")
	}
	s.addApp(s.router.PathPrefix("/api/blobstore").Subrouter(), blobstore)
	return s, nil
}

func (s *Server) Serve() error {
	return http.ListenAndServe(":8000", s.router)
}

func (s *Server) addApp(r *mux.Router, app Apper) error {
	s.apps[app.Name()] = app
	for _, reqName := range app.Required() {
		if _, ok := s.apps[reqName]; !ok {
			return fmt.Errorf("missing %s requirement", reqName)
		}
		app.AddHook(reqName, s.apps[reqName])
	}
	// app.Register(r)
	return nil
}
