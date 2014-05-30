package config

import (
	"fmt"
	"github.com/tsileo/datadatabase/backend"

	"github.com/tsileo/datadatabase/backend/blobsfile"
	"github.com/tsileo/datadatabase/backend/encrypt"

	"github.com/bitly/go-simplejson"
)

// TODO move this into another package and move the new from config here

func NewEncryptFromConfig(conf *simplejson.Json) backend.BlobHandler {
	return encrypt.New(conf.Get("key-path").MustString(), NewFromConfig(conf.Get("dest")))
}

func NewFromConfig(conf *simplejson.Json) backend.BlobHandler {
	backendType := conf.Get("backend-type").MustString("")
	if backendType == "" {
		panic(fmt.Errorf("backend-type key missing from backend config"))
	}
	backendArgs, ok := conf.CheckGet("backend-args")
	if !ok {
		panic(fmt.Errorf("backend-args key missing from backend config %v", backendType))
	}
	switch {
	case backendType == "blobsfile":
		return blobsfile.NewFromConfig(backendArgs)
	case backendType == "encrypt":
		return NewEncryptFromConfig(backendArgs)
	default:
		panic(fmt.Errorf("backend %v unknown", backendType))
	}
	return nil
}
