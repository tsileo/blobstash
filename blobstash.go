package main

import (
	"flag"
	"log"

	"a4.io/blobstash/pkg/config"
	"a4.io/blobstash/pkg/server"
)

var (
	scan     bool
	loglevel string
	err      error
)

func main() {
	flag.BoolVar(&scan, "scan", false, "Trigger a BlobStore rescan.")
	flag.StringVar(&loglevel, "loglevel", "", "logging level (debug|info|warn|crit)")
	flag.Parse()
	conf := &config.Config{}
	if flag.NArg() == 1 {
		conf, err = config.New(flag.Arg(0))
		if err != nil {
			log.Fatalf("failed to load config at \"%v\": %v", flag.Arg(0), err)
		}
	}

	// Set the ScanMode in the config
	conf.ScanMode = scan
	if loglevel != "" {
		conf.LogLevel = loglevel
	}

	s, err := server.New(conf)
	if err != nil {
		log.Fatalf("failed to initialize server: %v", err)
	}

	if err := s.Bootstrap(); err != nil {
		log.Fatalf("failed: %v", err)
	}

	if err := s.Serve(); err != nil {
		log.Fatalf("failed: %v", err)
	}
}
