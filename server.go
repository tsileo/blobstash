package main

import (
	"github.com/tsileo/datadatabase/server"
	"github.com/tsileo/datadatabase/backend"
)

func main() {
	stop := make(chan bool)
	keyPath := "/work/opensource/homedb_gopath/src/github.com/tsileo/datadatabase/keytest.key"
	blobBackend := backend.NewLocalBackend("./tmp_blobs_enc")
	encBlobBackend := backend.NewEncryptBackend(keyPath, blobBackend)
	metaBackend := backend.NewLocalBackend("./tmp_meta")
	server.New("127.0.0.1:9736", "./tmp_db", encBlobBackend, metaBackend, false, stop)
}
