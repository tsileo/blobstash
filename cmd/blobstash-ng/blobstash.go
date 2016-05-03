package main

import (
	"log"

	"github.com/tsileo/blobstash/pkg/server"
)

func main() {
	s, err := server.New()
	if err != nil {
		log.Fatalf("failed to initialize server: %v", err)
	}
	if err := s.Serve(); err != nil {
		log.Fatalf("failed: %v", err)
	}
}
