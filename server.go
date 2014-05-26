package main

import (
	"github.com/tsileo/datadatabase/server"
	_ "github.com/tsileo/datadatabase/backend/blobsfile"
	"github.com/tsileo/datadatabase/backend/s3"
)

func main() {
	stop := make(chan bool)
	//keyPath := "/work/opensource/homedb_gopath/src/github.com/tsileo/datadatabase/keytest.key"
	//blobBackend := blobsfile.New("/box/tmp_blobsfile")
	blobBackend := s3.New("thomassileotestbackndblobs")
	metaBackend := s3.New("thomassileotestbackndbmeta")
	//encBlobBackend := backend.NewEncryptBackend(keyPath, blobBackend)
	//metaBackend := blobsfile.New("/box/tmp_blobsfile_meta")
	server.New("127.0.0.1:9736", "./tmp_db3", blobBackend, metaBackend, false, stop)
}
