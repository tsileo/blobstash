package config2

import (
	"fmt"

	"github.com/tsileo/blobstash/backend"
	"github.com/tsileo/blobstash/backend/blobsfile"
	"github.com/tsileo/blobstash/backend/encrypt"
	"github.com/tsileo/blobstash/backend/glacier"
	"github.com/tsileo/blobstash/backend/mirror"
	"github.com/tsileo/blobstash/backend/s3"
)

const defaultS3Location = "us-east-1"

// TODO move this into another package and move the new from config here

func NewEncryptFromConfig(conf map[string]interface{}) backend.BlobHandler {
	return encrypt.New(conf["key-path"].(string), NewFromConfig(conf["dest"].(map[string]interface{})))
}

func NewS3FromConfig(conf map[string]interface{}) backend.BlobHandler {
	bucket := conf["bucket"].(string)
	if bucket == "" {
		panic(fmt.Errorf("no bucket specified for S3Backend"))
	}
	location := defaultS3Location
	_, locationOk := conf["location"]
	if locationOk {
		location = conf["location"].(string)
	}
	return s3.New(bucket, location)
}

func NewGlacierFromConfig(conf map[string]interface{}) backend.BlobHandler {
	vault := conf["vault"].(string)
	if vault == "" {
		panic(fmt.Errorf("no vault specified for GalcierBackend"))
	}
	region := conf["region"].(string)
	cacheDir := fmt.Sprintf("glacier-cache-%v", vault)
	_, cacheDirOk := conf["cache-dir"]
	if cacheDirOk {
		cacheDir = conf["cache-dir"].(string)
	}
	compression := false
	_, compressionOk := conf["compression"]
	if compressionOk {
		compression = conf["compression"].(bool)
	}
	return glacier.New(vault, region, cacheDir, compression)
}

func NewMirrorFromConfig(conf map[string]interface{}) backend.BlobHandler {
	backends := []backend.BlobHandler{}
	backs := conf["backends"]
	if backs != nil {
		for _, b := range backs.([]interface{}) {
			bconf := b.(map[string]interface{})
			backends = append(backends, NewFromConfig(bconf))
		}
	}
	wbackends := []backend.BlobHandler{}
	backs = conf["write-backends"]
	if backs != nil {
		for _, b := range backs.([]interface{}) {
			bconf := b.(map[string]interface{})
			wbackends = append(wbackends, NewFromConfig(bconf))
		}
	}
	return mirror.New(backends, wbackends)
}

func NewFromConfig(conf map[string]interface{}) backend.BlobHandler {
	backendType := conf["backend-type"].(string)
	if backendType == "" {
		panic(fmt.Errorf("backend-type key missing from backend config %+v", conf))
	}
	_, ok := conf["backend-args"]
	if !ok {
		panic(fmt.Errorf("backend-args key missing from backend config %v", backendType))
	}
	backendArgs := conf["backend-args"].(map[string]interface{})
	switch {
	case backendType == "blobsfile":
		return blobsfile.NewFromConfig2(backendArgs)
	case backendType == "glacier":
		return NewGlacierFromConfig(backendArgs)
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
