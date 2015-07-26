package server

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/tsileo/blobstash/api"
	"github.com/tsileo/blobstash/backend"
	"github.com/tsileo/blobstash/config"
	"github.com/tsileo/blobstash/config/pathutil"
	"github.com/tsileo/blobstash/embed"
	"github.com/tsileo/blobstash/meta"
	"github.com/tsileo/blobstash/router"
	"github.com/tsileo/blobstash/vkv"
)

var Version = "0.0.0"

var DefaultConf = map[string]interface{}{
	"backends": map[string]interface{}{
		"blobs": map[string]interface{}{
			"backend-type": "blobsfile",
			"backend-args": map[string]interface{}{
				"path": "blobs",
			},
		},
	},
	"router": []interface{}{[]interface{}{"default", "blobs"}},
}

type Server struct {
	Router      *router.Router
	Backends    map[string]backend.BlobHandler
	DB          *vkv.DB
	metaHandler *meta.MetaHandler

	KvUpdate chan *vkv.KeyValue
	blobs    chan *router.Blob

	shutdown chan struct{}
	stop     chan struct{}
	wg       sync.WaitGroup
}

func New(conf map[string]interface{}) *Server {
	if conf == nil {
		conf = DefaultConf
	}
	vardir := pathutil.VarDir()
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
		shutdown: make(chan struct{}, 1),
		stop:     make(chan struct{}),
		blobs:    make(chan *router.Blob),
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

func (s *Server) Embed() {
	// Start meta handler: watch for kv update and create meta blob
	go s.metaHandler.WatchKvUpdate(s.wg, s.blobs, s.KvUpdate)
	// Scan existing meta blob
	go func() {
		s.wg.Add(1)
		defer s.wg.Done()
		if err := s.metaHandler.Scan(); err != nil {
			panic(err)
		}
	}()
	// Start the worker for handling blob upload
	for i := 0; i < 25; i++ {
		go s.processBlobs()
	}
}

// KvStore return a kv client, written for embedded client
func (s *Server) KvStore() *embed.KvStore {
	return embed.NewKvStore(s.DB, s.KvUpdate, s.Router)
}

func (s *Server) BlobStore() *embed.BlobStore {
	return embed.NewBlobStore(s.blobs, s.Router)
}

func (s *Server) Run() error {
	// Start the HTTP API
	r := api.New(s.wg, s.DB, s.KvUpdate, s.Router, s.blobs)
	http.Handle("/", r)
	log.Printf("server: HTTP API listening on 0.0.0.0:8050")
	go func() {
		if err := http.ListenAndServe(":8050", nil); err != nil {
			panic(err)
		}
	}()
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
			log.Printf("server: Captured %v\n", sig)
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
