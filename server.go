package main

import (
	"github.com/tsileo/datadatabase/server"
	"github.com/tsileo/datadatabase/backend"
	"os"
)

func main() {
	stop := make(chan bool)
	blobBackend := backend.NewLocalBackend("./tmp_blobs")
	defer os.RemoveAll("./tmp_blobs")
	server.New("127.0.0.1:9736", "./tmp_db", blobBackend, false, stop)
}
