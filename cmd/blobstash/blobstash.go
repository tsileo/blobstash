package main

import (
	"flag"
	"log"

	"github.com/tsileo/blobstash/pkg/config"
	"github.com/tsileo/blobstash/pkg/server"
)

func main() {
	flag.Parse()
	var err error
	// fmt.Printf("%d", os.NA
	conf := &config.Config{}
	if flag.NArg() == 1 {
		conf, err = config.New(flag.Arg(0))
		if err != nil {
			log.Fatalf("failed to load config at \"%v\": %v", flag.Arg(0), err)
		}
	}
	s, err := server.New(conf)
	if err != nil {
		log.Fatalf("failed to initialize server: %v", err)
	}
	if err := s.Serve(); err != nil {
		log.Fatalf("failed: %v", err)
	}
}
