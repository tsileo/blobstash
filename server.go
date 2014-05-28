package main

import (
	"github.com/tsileo/datadatabase/server"
	_ "github.com/tsileo/datadatabase/backend/blobsfile"
	"github.com/tsileo/datadatabase/backend/s3"
	"github.com/tsileo/datadatabase/backend/encrypt"
)

func main() {
	stop := make(chan bool)
	keyPath := "/home/thomas/genkey/datadb_key"
	blobBackend := encrypt.New(keyPath, s3.New("thomassileodatadbblobst", "eu-west-1"))
	metaBackend := encrypt.New(keyPath, s3.New("thomassileodatadbmetat", "eu-west-1"))
	server.New("127.0.0.1:9736", "./tmp_db", blobBackend, metaBackend, false, stop)
}
