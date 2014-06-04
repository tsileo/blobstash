package config

import (
	"fmt"

	"github.com/bitly/go-simplejson"

	"github.com/tsileo/datadatabase/backend"
	"github.com/tsileo/datadatabase/backend/blobsfile"
	"github.com/tsileo/datadatabase/backend/encrypt"
	"github.com/tsileo/datadatabase/backend/mirror"
	"github.com/tsileo/datadatabase/backend/s3"
)

const defaultS3Location = "us-east-1"

// TODO move this into another package and move the new from config here

func NewEncryptFromConfig(conf *simplejson.Json) backend.BlobHandler {
	return encrypt.New(conf.Get("key-path").MustString(), NewFromConfig(conf.Get("dest")))
}

func NewS3FromConfig(conf *simplejson.Json) backend.BlobHandler {
	bucket := conf.Get("bucket").MustString()
	if bucket == "" {
		panic(fmt.Errorf("no bucket specified for S3Backend"))
	}
	return s3.New(bucket, conf.Get("location").MustString(defaultS3Location))
}

func NewMirrorFromConfig(conf *simplejson.Json) backend.BlobHandler {
	backends := []backend.BlobHandler{}
	for index, _ := range conf.Get("backends").MustArray() {
		backends = append(backends, NewFromConfig(conf.Get("backends").GetIndex(index)))
	}
	return mirror.New(backends...)
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
	case backendType == "s3":
		return NewS3FromConfig(backendArgs)
	case backendType == "mirror":
		return NewMirrorFromConfig(backendArgs)
	default:
		panic(fmt.Errorf("backend %v unknown", backendType))
	}
	return nil
}
