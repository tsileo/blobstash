package server

import (
	"crypto/rand"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/carbocation/interpose/middleware"
	"github.com/gorilla/mux"
	"github.com/rs/cors"
	"github.com/tsileo/blobstash/api"
	"github.com/tsileo/blobstash/backend"
	"github.com/tsileo/blobstash/config"
	"github.com/tsileo/blobstash/config/pathutil"
	"github.com/tsileo/blobstash/embed"
	"github.com/tsileo/blobstash/ext/docstore"
	"github.com/tsileo/blobstash/ext/lua"
	"github.com/tsileo/blobstash/httputil"
	"github.com/tsileo/blobstash/logger"
	"github.com/tsileo/blobstash/meta"
	serverMiddleware "github.com/tsileo/blobstash/middleware"
	"github.com/tsileo/blobstash/router"
	"github.com/tsileo/blobstash/vkv"
	"github.com/tsileo/blobstash/vkv/hub"
	log2 "gopkg.in/inconshreveable/log15.v2"
)

var Version = "0.0.0"

var DefaultConf = map[string]interface{}{
	"backends": map[string]interface{}{
		"blobs": map[string]interface{}{
			"backend-type": "blobsfile",
			"backend-args": map[string]interface{}{
				"path": "$VAR/blobs",
			},
		},
	},
	"router":    []interface{}{[]interface{}{"default", "blobs"}},
	"data_path": pathutil.VarDir(),
}

// FIXME(ts) create ext interface with io.Closer and store them in a map as server attribute.

type Server struct {
	Log         log2.Logger
	Router      *router.Router
	Backends    map[string]backend.BlobHandler
	DB          *vkv.DB
	metaHandler *meta.MetaHandler

	KvUpdate chan *vkv.KeyValue
	blobs    chan *router.Blob

	resync   bool
	ready    chan struct{}
	shutdown chan struct{}
	stop     chan struct{}
	wg       sync.WaitGroup
	watchHub *hub.Hub
}

func New(conf map[string]interface{}) *Server {
	if conf == nil {
		conf = DefaultConf
	}
	vardir := pathutil.VarDir()
	if dpath, ok := conf["data_path"].(string); ok {
		vardir = dpath
	}
	os.MkdirAll(vardir, 0700)
	db, err := vkv.New(filepath.Join(vardir, "vkv.db"))
	if err != nil {
		panic(err)
	}
	server := &Server{
		Router:   router.New(conf["router"].([]interface{})),
		Backends: map[string]backend.BlobHandler{},
		DB:       db,
		KvUpdate: make(chan *vkv.KeyValue),
		ready:    make(chan struct{}, 1),
		shutdown: make(chan struct{}, 1),
		stop:     make(chan struct{}),
		blobs:    make(chan *router.Blob),
		resync:   conf["resync"].(bool),
		Log:      logger.Log,
		watchHub: hub.NewHub(),
	}
	backends := conf["backends"].(map[string]interface{})
	for _, b := range server.Router.ResolveBackends() {
		server.Backends[b] = config.NewFromConfig(backends[b].(map[string]interface{}))
		server.Router.Backends[b] = server.Backends[b]
	}
	server.metaHandler = meta.New(server.Router, server.DB)
	return server
}

func (s *Server) processBlobs() {
	s.wg.Add(1)
	defer s.wg.Done()
	for {
		select {
		case blob := <-s.blobs:
			log.Printf("processBlobs: %+v", blob)
			backend := s.Router.Route(blob.Req)
			exists, err := backend.Exists(blob.Hash)
			if err != nil {
				panic(fmt.Errorf("processBlobs error: %v", err))
			}
			if !exists {
				if err := backend.Put(blob.Hash, blob.Blob); err != nil {
					panic(fmt.Errorf("processBlobs error: %v", err))
				}
			}
		case <-s.stop:
			return
		}
	}
}

// TillReady blocks until all blobs get scanned (and all meta blobs applied if needed)
func (s *Server) TillReady() {
	<-s.ready
}

// SetUp should be called instead of Run in embedded mode
func (s *Server) SetUp() {
	// Start meta handler: watch for kv update and create meta blob
	go s.metaHandler.WatchKvUpdate(s.wg, s.blobs, s.KvUpdate, s.watchHub)
	// Scan existing meta blob
	if s.resync {
		go func() {
			s.wg.Add(1)
			defer s.wg.Done()
			if err := s.metaHandler.Scan(); err != nil {
				panic(err)
			}
			s.ready <- struct{}{}
		}()
	} else {
		s.ready <- struct{}{}
	}
	// Start the worker for handling blob upload
	for i := 0; i < 25; i++ {
		go s.processBlobs()
	}
}

// KvStore return a kv client, written for embedded client
func (s *Server) KvStore() *embed.KvStore {
	return embed.NewKvStore(s.DB, s.KvUpdate, s.Router)
}

// BlobStore returns a blob store clienm for embedded usge
func (s *Server) BlobStore() *embed.BlobStore {
	return embed.NewBlobStore(s.blobs, s.Router)
}

// Run runs the server and block until the server is shutdown
func (s *Server) Run() {
	// XXX(tsileo) make the key persisiting between restart?
	// TODO retrive both key from the KvStore
	hawkKey := make([]byte, 64)
	if _, err := rand.Read(hawkKey); err != nil {
		panic(err)
	}
	// Set up auth
	// TODO Try to retrieve the API key from the KvStore or generate a new (UUID v4)
	// Maybe handle a master key with multiple key? so key can be revoked?
	authMiddleware := middleware.BasicAuth("", "token")
	middlewares := &serverMiddleware.SharedMiddleware{
		Auth: authMiddleware,
	}
	// FIXME token as parameter
	authFunc := httputil.BasicAuthFunc("", "token")
	// Start the HTTP API
	s.SetUp()
	r := mux.NewRouter()
	// publicRoute := r.PathPrefix("/public").Subrouter()
	appRoute := r.PathPrefix("/app").Subrouter()
	ekvstore := s.KvStore()
	eblobstore := s.BlobStore()
	docstore.New(s.Log.New("ext", "docstore"), ekvstore, eblobstore).RegisterRoute(r.PathPrefix("/api/ext/docstore/v1").Subrouter(), middlewares)
	luaExt := lua.New(s.Log.New("ext", "lua"), hawkKey, authFunc, ekvstore, eblobstore)
	luaExt.RegisterRoute(r.PathPrefix("/api/ext/lua/v1").Subrouter(), middlewares)
	luaExt.RegisterAppRoute(appRoute, middlewares)
	api.New(r.PathPrefix("/api/v1").Subrouter(), middlewares, s.wg, s.DB, s.KvUpdate, s.Router, s.blobs, s.watchHub)

	// FIXME allowedorigins from config
	c := cors.New(cors.Options{
		AllowedOrigins: []string{"*"},
	})
	http.Handle("/", c.Handler(r))
	s.Log.Info("server: HTTP API listening on 0.0.0.0:8050")
	go func() {
		if err := http.ListenAndServe(":8050", nil); err != nil {
			panic(err)
		}
	}()
	s.TillShutdown()
}

// TillShutdown blocks until a kill signal is catched.
func (s *Server) TillShutdown() {
	// Listen for shutdown signal
	cs := make(chan os.Signal, 1)
	signal.Notify(cs, os.Interrupt,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	for {
		select {
		case <-s.shutdown:
			break
		case sig := <-cs:
			s.Log.Debug("captured signal", "signal", sig)
			s.Log.Info("shutting down...")
			break
		}
		s.Close()
		os.Exit(0)
	}
}

func (s *Server) Close() {
	close(s.stop)
	s.wg.Wait()
	close(s.KvUpdate)
	s.DB.Close()
	for _, b := range s.Backends {
		b.Close()
	}
}
