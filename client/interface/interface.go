package client

import "github.com/tsileo/blobstash/client/response"

type KvStorer interface {
	Put(string, string, int) (*response.KeyValue, error)
	Get(string, int) (*response.KeyValue, error)
	Versions(string, int, int, int) (*response.KeyValueVersions, error)
}

type BlobStorer interface {
	Get(string) ([]byte, error)
	Enumerate(chan<- string, string, string, int) error
	Stat(string) (bool, error)
	Put(string, []byte) error
	WaitBlobs()
	ProcessBlobs()
}
