package server // import "a4.io/blobstash/pkg/server"

import (
	"context"
	"errors"
	"expvar"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"

	"a4.io/blobstash/pkg/apps"
	"a4.io/blobstash/pkg/auth"
	"a4.io/blobstash/pkg/blobstore"
	blobStoreAPI "a4.io/blobstash/pkg/blobstore/api"
	"a4.io/blobstash/pkg/capabilities"
	"a4.io/blobstash/pkg/config"
	"a4.io/blobstash/pkg/docstore"
	docstoreLua "a4.io/blobstash/pkg/docstore/lua"
	"a4.io/blobstash/pkg/expvarserver"
	"a4.io/blobstash/pkg/filetree"
	"a4.io/blobstash/pkg/gitserver"
	"a4.io/blobstash/pkg/httputil"
	"a4.io/blobstash/pkg/hub"
	"a4.io/blobstash/pkg/kvstore"
	kvStoreAPI "a4.io/blobstash/pkg/kvstore/api"
	"a4.io/blobstash/pkg/meta"
	"a4.io/blobstash/pkg/middleware"
	"a4.io/blobstash/pkg/oplog"
	"a4.io/blobstash/pkg/replication"
	"a4.io/blobstash/pkg/stash"
	stashAPI "a4.io/blobstash/pkg/stash/api"
	synctable "a4.io/blobstash/pkg/sync"

	"golang.org/x/crypto/acme/autocert"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	log "github.com/inconshreveable/log15"
	lua "github.com/yuin/gopher-lua"
)

var serverCounters = expvar.NewMap("server")

func pingHandler(w http.ResponseWriter, r *http.Request) {
	httputil.MarshalAndWrite(r, w, map[string]interface{}{
		"ping": "pong",
	})
}

type App interface {
	Register(*mux.Router, func(http.Handler) http.Handler)
}

type Server struct {
	router    *mux.Router
	conf      *config.Config
	log       log.Logger
	closeFunc func() error

	blobstore *blobstore.BlobStore

	hostWhitelist map[string]bool
	shutdown      chan struct{}
	wg            *sync.WaitGroup
}

func New(conf *config.Config) (*Server, error) {
	conf.Init()
	logger := log.New("logger", "blobstash")
	if err := auth.Setup(conf, logger.New("app", "perms")); err != nil {
		return nil, fmt.Errorf("failed to setup auth: %v", err)
	}
	logger.SetHandler(log.LvlFilterHandler(conf.LogLvl(), log.StreamHandler(os.Stdout, log.TerminalFormat())))
	var wg sync.WaitGroup
	s := &Server{
		router:        mux.NewRouter().StrictSlash(true),
		conf:          conf,
		hostWhitelist: map[string]bool{},
		log:           logger,
		wg:            &wg,
		shutdown:      make(chan struct{}),
	}
	authFunc, basicAuth := middleware.NewBasicAuth(conf)
	s.router.Handle("/api/ping", basicAuth(http.HandlerFunc(pingHandler)))

	hub := hub.New(logger.New("app", "hub"), true)
	// Load the blobstore
	rootBlobstore, err := blobstore.New(logger.New("app", "blobstore"), true, conf.VarDir(), conf, hub)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize blobstore app: %v", err)
	}
	s.blobstore = rootBlobstore

	s.router.Handle("/api/status", basicAuth(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		stats, err := s.blobstore.S3Stats()
		if err != nil {
			if err != blobstore.ErrRemoteNotAvailable {
				panic(err)
			}
		}

		// return newRev.Version, nil
		httputil.MarshalAndWrite(r, w, map[string]interface{}{
			"s3": stats,
		})

	})))

	// Load the meta
	metaHandler, err := meta.New(logger.New("app", "meta"), hub)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize blobstore meta: %v", err)
	}

	if conf.Replication != nil && conf.Replication.EnableOplog {
		oplg, err := oplog.New(logger.New("app", "oplog"), conf, hub)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize oplog: %v", err)
		}
		oplg.Register(s.router.PathPrefix("/_oplog").Subrouter(), basicAuth)
	}
	// Load the kvstore
	rootKvstore, err := kvstore.New(logger.New("app", "kvstore"), conf.VarDir(), rootBlobstore, metaHandler)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize kvstore app: %v", err)
	}

	// Now load the stash manager
	// func New(dir string, m *meta.Meta, bs *blobstore.BlobStore, kvs *kvstore.KvStore, h *hub.Hub, l log.Logger) (*Stash, error) {
	cstash, err := stash.New(conf.StashDir(), metaHandler, rootBlobstore, rootKvstore, hub, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize the stash manager: %v", err)
	}
	stashAPI.New(cstash, hub).Register(s.router.PathPrefix("/api/stash").Subrouter(), basicAuth)

	blobstore := cstash.BlobStore()
	// FIXME(tsileo): test the stash with kvstore
	//kvstore := rootKvstore
	kvstore := cstash.KvStore()

	kvStoreAPI.New(kvstore).Register(s.router.PathPrefix("/api/kvstore").Subrouter(), basicAuth)
	// FIXME(tsileo): handle middleware in the `Register` interface
	blobStoreAPI.New(blobstore).Register(s.router.PathPrefix("/api/blobstore").Subrouter(), basicAuth)

	// Load the synctable
	// XXX(tsileo): sync should always get the root data context
	synctable := synctable.New(logger.New("app", "sync"), conf, rootBlobstore)
	synctable.Register(s.router.PathPrefix("/api/sync").Subrouter(), basicAuth)

	// Enable replication if set in the config
	if conf.ReplicateFrom != nil {
		if _, err := replication.New(logger.New("app", "replication"), conf, rootBlobstore, synctable, &wg); err != nil {
			return nil, fmt.Errorf("failed to initialize replication app: %v", err)
		}
	}

	filetree, err := filetree.New(logger.New("app", "filetree"), conf, authFunc, kvstore, blobstore, hub, rootBlobstore.GetRemoteRef)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize filetree app: %v", err)
	}
	filetree.Register(s.router.PathPrefix("/api/filetree").Subrouter(), s.router, basicAuth)

	docstore, err := docstore.New(logger.New("app", "docstore"), conf, kvstore, blobstore, filetree)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize docstore app: %v", err)
	}
	docstore.Register(s.router.PathPrefix("/api/docstore").Subrouter(), basicAuth)

	git, err := gitserver.New(logger.New("app", "gitserver"), conf, kvstore, blobstore, hub)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize git server app: %v", err)
	}
	git.Register(s.router.PathPrefix("/api/git").Subrouter(), s.router, basicAuth)

	// Load the Lua config
	if _, err := os.Stat("blobstash.lua"); err == nil {
		if err := func() error {
			L := lua.NewState()
			defer L.Close()
			docstoreLua.Setup(L, docstore)
			dat, err := ioutil.ReadFile("blobstash.lua")
			if err != nil {
				return err
			}
			if err := L.DoString(string(dat)); err != nil {
				return fmt.Errorf("failed to load blobstash.lua: %v", err)
			}
			return nil
		}(); err != nil {
			return nil, err
		}
	}

	apps, err := apps.New(logger.New("app", "apps"), conf, rootBlobstore, kvstore, filetree, docstore, git, hub, s.whitelistHosts)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize filetree app: %v", err)
	}
	apps.Register(s.router.PathPrefix("/api/apps").Subrouter(), s.router, basicAuth)

	caps, err := capabilities.New(logger.New("app", "caps"), conf, rootBlobstore, hub)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize caps app: %v", err)
	}
	caps.Register(s.router.PathPrefix("/api/capabilities").Subrouter(), basicAuth)

	// Setup the closeFunc
	s.closeFunc = func() error {
		logger.Debug("waiting for the waitgroup...")
		wg.Wait()
		logger.Debug("waitgroup done")
		if err := filetree.Close(); err != nil {
			return err
		}
		logger.Debug("filetree closed")
		if err := docstore.Close(); err != nil {
			return err
		}
		logger.Debug("docstore closed")
		if err := apps.Close(); err != nil {
			return err
		}
		logger.Debug("apps closed")
		if err := cstash.Close(); err != nil {
			return err
		}
		logger.Debug("stash closed")
		if err := rootKvstore.Close(); err != nil {
			return err
		}
		logger.Debug("root kv closed")
		if err := rootBlobstore.Close(); err != nil {
			return err
		}
		logger.Debug("root bs closed")
		return nil
	}
	return s, nil
}

func (s *Server) Shutdown() {
	s.shutdown <- struct{}{}
	// TODO(tsileo) shotdown sync repl too
}

func (s *Server) Bootstrap() error {
	s.log.Debug("Bootstrap the server")

	// Check if a full scan is requested
	if s.conf.ScanMode {
		s.log.Info("Starting full scan")
		if err := s.blobstore.Scan(context.Background()); err != nil {
			return err
		}
		s.log.Info("Scan done")
	}
	return nil
}

func (s *Server) hostPolicy(hosts ...string) autocert.HostPolicy {
	s.whitelistHosts(hosts...)
	return func(_ context.Context, host string) error {
		if !s.hostWhitelist[host] {
			return errors.New("blobstash: tls host not configured")
		}
		return nil
	}
}

func (s *Server) whitelistHosts(hosts ...string) {
	for _, h := range hosts {
		s.hostWhitelist[h] = true
	}
}

func (s *Server) Serve() error {
	reqLogger := httputil.LoggerMiddleware(s.log)
	expvarMiddleare := httputil.ExpvarsMiddleware(serverCounters)
	h := httputil.RecoverHandler(middleware.CorsMiddleware(reqLogger(expvarMiddleare(middleware.Secure(s.router)))))
	if s.conf.ExtraApacheCombinedLogs != "" {
		s.log.Info(fmt.Sprintf("enabling apache logs to %s", s.conf.ExtraApacheCombinedLogs))
		logFile, err := os.OpenFile(s.conf.ExtraApacheCombinedLogs, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
		defer logFile.Close()
		h = handlers.CombinedLoggingHandler(logFile, h)
	}

	go func() {
		listen := config.DefaultListen
		if s.conf.Listen != "" {
			listen = s.conf.Listen
		}
		s.log.Info(fmt.Sprintf("listening on %v", listen))
		if s.conf.AutoTLS {
			cacheDir := autocert.DirCache(filepath.Join(s.conf.ConfigDir(), config.LetsEncryptDir))

			m := autocert.Manager{
				Prompt:     autocert.AcceptTOS,
				HostPolicy: s.hostPolicy(s.conf.Domains...),
				Cache:      cacheDir,
			}
			s := &http.Server{
				Addr:      listen,
				Handler:   h,
				TLSConfig: m.TLSConfig(),
			}
			s.ListenAndServeTLS("", "")
		} else {
			http.ListenAndServe(listen, h)
		}
	}()
	if s.conf.ExpvarListen != "" {
		go func() {
			s.log.Info(fmt.Sprintf("enabling expvar server on %v", s.conf.ExpvarListen))
			if err := expvarserver.Enable(s.conf); err != nil {
				s.log.Info(fmt.Sprintf("failed: %v", err))
			}
		}()
	}
	s.tillShutdown()
	return s.closeFunc()
	// return http.ListenAndServe(":8051", s.router)
}

func (s *Server) tillShutdown() {
	// Listen for shutdown signal
	cs := make(chan os.Signal, 1)
	signal.Notify(cs, os.Interrupt,
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
