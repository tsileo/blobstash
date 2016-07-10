package server

import (
	"crypto/tls"
	"fmt"
	_ "io"
	_ "log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/tsileo/blobstash/pkg/apps"
	"github.com/tsileo/blobstash/pkg/blobstore"
	"github.com/tsileo/blobstash/pkg/config"
	"github.com/tsileo/blobstash/pkg/docstore"
	"github.com/tsileo/blobstash/pkg/filetree"
	"github.com/tsileo/blobstash/pkg/httputil"
	"github.com/tsileo/blobstash/pkg/hub"
	"github.com/tsileo/blobstash/pkg/kvstore"
	"github.com/tsileo/blobstash/pkg/meta"
	"github.com/tsileo/blobstash/pkg/middleware"
	"github.com/tsileo/blobstash/pkg/nsdb"
	"github.com/tsileo/blobstash/pkg/synctable"

	"github.com/dkumor/acmewrapper"
	"github.com/gorilla/mux"
	log "github.com/inconshreveable/log15"
	"github.com/xenolf/lego/acme"
)

type App interface {
	Register(*mux.Router, func(http.Handler) http.Handler)
}

type Server struct {
	router    *mux.Router
	conf      *config.Config
	log       log.Logger
	closeFunc func() error

	shutdown chan struct{}
	wg       sync.WaitGroup
}

func New(conf *config.Config) (*Server, error) {
	conf.Init()
	logger := log.New()
	logger.SetHandler(log.StreamHandler(os.Stdout, log.TerminalFormat()))
	var wg sync.WaitGroup
	s := &Server{
		router:   mux.NewRouter().StrictSlash(true),
		conf:     conf,
		log:      logger,
		wg:       wg,
		shutdown: make(chan struct{}),
	}
	authFunc, basicAuth := middleware.NewBasicAuth(conf)
	hub := hub.New(logger.New("app", "hub"))
	// Load the blobstore
	blobstore, err := blobstore.New(logger.New("app", "blobstore"), conf, hub, wg)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize blobstore app: %v", err)
	}
	// FIXME(tsileo): handle middleware in the `Register` interface
	blobstore.Register(s.router.PathPrefix("/api/blobstore").Subrouter(), basicAuth)
	// Load the meta
	metaHandler, err := meta.New(logger.New("app", "meta"), hub)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize blobstore meta: %v", err)
	}
	// Load the kvstore
	kvstore, err := kvstore.New(logger.New("app", "kvstore"), conf, blobstore, metaHandler)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize kvstore app: %v", err)
	}
	kvstore.Register(s.router.PathPrefix("/api/kvstore").Subrouter(), basicAuth)
	nsDB, err := nsdb.New(logger.New("app", "nsdb"), conf, blobstore, metaHandler, hub)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize nsdb: %v", err)
	}
	// Load the synctable
	synctable := synctable.New(logger.New("app", "sync"), conf, blobstore, nsDB)
	synctable.Register(s.router.PathPrefix("/api/sync").Subrouter(), basicAuth)

	filetree, err := filetree.New(logger.New("app", "filetree"), conf, authFunc, kvstore, blobstore)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize filetree app: %v", err)
	}
	filetree.Register(s.router.PathPrefix("/api/filetree").Subrouter(), s.router, basicAuth)

	apps, err := apps.New(logger.New("app", "apps"), conf)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize filetree app: %v", err)
	}
	apps.Register(s.router.PathPrefix("/api/apps").Subrouter(), s.router, basicAuth)

	docstore, err := docstore.New(logger.New("app", "docstore"), conf, kvstore, blobstore)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize docstore app: %v", err)
	}
	docstore.Register(s.router.PathPrefix("/api/docstore").Subrouter(), basicAuth)

	// Setup the closeFunc
	s.closeFunc = func() error {
		logger.Debug("waiting for the waitgroup...")
		wg.Wait()
		logger.Debug("waitgroup done")

		if err := blobstore.Close(); err != nil {
			return err
		}
		if err := kvstore.Close(); err != nil {
			return err
		}
		if err := nsDB.Close(); err != nil {
			return err
		}
		if err := filetree.Close(); err != nil {
			return err
		}
		if err := docstore.Close(); err != nil {
			return err
		}
		if err := apps.Close(); err != nil {
			return err
		}
		return nil
	}
	return s, nil
}

func (s *Server) Shutdown() {
	s.shutdown <- struct{}{}
}

func (s *Server) Serve() error {
	go func() {
		listen := config.DefaultListen
		if s.conf.Listen != "" {
			listen = s.conf.Listen
		}
		s.log.Info(fmt.Sprintf("listening on %v", listen))
		reqLogger := httputil.LoggerMiddleware(s.log)
		h := httputil.RecoverHandler(middleware.CorsMiddleware(reqLogger(middleware.Secure(s.router))))

		if s.conf.AutoTLS {
			w, err := acmewrapper.New(acmewrapper.Config{
				Domains: s.conf.Domains,
				Address: listen,

				TLSCertFile: filepath.Join(s.conf.ConfigDir(), config.LetsEncryptDir, "cert.pem"),
				TLSKeyFile:  filepath.Join(s.conf.ConfigDir(), config.LetsEncryptDir, "key.pem"),

				RegistrationFile: filepath.Join(s.conf.ConfigDir(), config.LetsEncryptDir, "user.reg"),
				PrivateKeyFile:   filepath.Join(s.conf.ConfigDir(), config.LetsEncryptDir, "user.pem"),
				PrivateKeyType:   acme.EC384, // Use `elliptic.P384()` for the private key
				TOSCallback:      acmewrapper.TOSAgree,
			})

			if err != nil {
				panic(err)
			}

			tlsconfig := w.TLSConfig()
			tlsconfig.MinVersion = tls.VersionSSL30

			listener, err := tls.Listen("tcp", listen, tlsconfig)
			if err != nil {
				panic(err)
			}

			// To enable http2, we need http.Server to have reference to tlsconfig
			// https://github.com/golang/go/issues/14374
			server := &http.Server{
				Addr:      listen,
				Handler:   h,
				TLSConfig: tlsconfig,
			}
			server.Serve(listener)
		} else {
			http.ListenAndServe(listen, h)
		}
	}()
	s.tillShutdown()
	return s.closeFunc()
	// return http.ListenAndServe(":8051", s.router)
}

func (s *Server) tillShutdown() {
	// Listen for shutdown signal
	cs := make(chan os.Signal, 1)
	signal.Notify(cs, os.Interrupt,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	for {
		select {
		case sig := <-cs:
			s.log.Debug("captured signal", "signal", sig)
			s.log.Info("shutting down...")
			return
		case <-s.shutdown:
			s.log.Info("shutting down...")
			return
		}
	}
}
