package main

import (
	"github.com/tsileo/datadatabase/server"
	"github.com/tsileo/datadatabase/backend/blobsfile"
)

func main() {
	stop := make(chan bool)
	//keyPath := "/work/opensource/homedb_gopath/src/github.com/tsileo/datadatabase/keytest.key"
	blobBackend := blobsfile.New("/box/tmp_blobsfile")
	defer blobBackend.Close()
	//encBlobBackend := backend.NewEncryptBackend(keyPath, blobBackend)
	metaBackend := blobsfile.New("/box/tmp_blobsfile_meta")
	defer metaBackend.Close()
	server.New("127.0.0.1:9736", "./tmp_db3", blobBackend, metaBackend, false, stop)
}
