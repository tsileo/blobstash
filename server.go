package main

import (
	"github.com/tsileo/datadatabase/server"
	"github.com/tsileo/datadatabase/backend"
)

func main() {
	stop := make(chan bool)
	//keyPath := "/work/opensource/homedb_gopath/src/github.com/tsileo/datadatabase/keytest.key"
	blobBackend := backend.NewLocalBackend("./tmp_blobs3")
	//encBlobBackend := backend.NewEncryptBackend(keyPath, blobBackend)
	metaBackend := backend.NewLocalBackend("./tmp_meta3")
	server.New("127.0.0.1:9736", "./tmp_db3", blobBackend, metaBackend, false, stop)
}
