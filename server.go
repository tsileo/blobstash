package main

import (
	"io/ioutil"
	"github.com/tsileo/datadatabase/server"
	
	"github.com/bitly/go-simplejson"

	"github.com/tsileo/datadatabase/config"
	"github.com/tsileo/datadatabase/backend/blobsfile"
	)

func main() {
	stop := make(chan bool)

	//keyPath := "/home/thomas/genkey/datadb_key"
	dat, _ := ioutil.ReadFile("config.json")
	conf, _ := simplejson.NewJson(dat)
	blobBackend := config.NewFromConfig(conf)
	metaBackend := blobsfile.New("/box/blobstoremetas")
	server.New("127.0.0.1:9736", "./tmp_db", blobBackend, metaBackend, stop)
}
