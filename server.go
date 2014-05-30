package main

import (
	"io/ioutil"
	"github.com/tsileo/datadatabase/server"
	
	"github.com/bitly/go-simplejson"

	"github.com/tsileo/datadatabase/config"
	"github.com/tsileo/datadatabase/backend/blobsfile"
	_ "github.com/tsileo/datadatabase/backend/s3"
	_ "github.com/tsileo/datadatabase/backend/encrypt"
)

func main() {
	stop := make(chan bool)

	//keyPath := "/home/thomas/genkey/datadb_key"
	//blobBackend := encrypt.New(keyPath, s3.New("thomassileodatadbblobst", "eu-west-1"))
	//metaBackend := encrypt.New(keyPath, s3.New("thomassileodatadbmetat", "eu-west-1"))
	dat, _ := ioutil.ReadFile("config.json")
	conf, _ := simplejson.NewJson(dat)
	blobBackend := config.NewFromConfig(conf)
	metaBackend := blobsfile.New("/box/blobstoremetas")
	server.New("127.0.0.1:9736", "./tmp_db", blobBackend, metaBackend, stop)
}
