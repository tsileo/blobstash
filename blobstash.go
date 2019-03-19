//go:generate go run tools/genluaindex.go
package main

import (
	"flag"
	"log"

	"a4.io/blobstash/pkg/config"
	"a4.io/blobstash/pkg/server"
)

var (
	scan      bool
	s3scan    bool
	s3restore bool
	check     bool
	loglevel  string
	err       error
)

func main() {
	flag.BoolVar(&check, "check", false, "Check the blobstore consistency.")
	flag.BoolVar(&scan, "scan", false, "Trigger a BlobStore rescan.")
	flag.BoolVar(&s3scan, "s3-scan", false, "Trigger a BlobStore rescan of the S3 backend.")
	flag.BoolVar(&s3restore, "s3-restore", false, "Trigger a BlobStore restore of the S3 backend.")
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
	conf.CheckMode = check
	conf.ScanMode = scan
	conf.S3ScanMode = s3scan
	conf.S3RestoreMode = s3restore
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
