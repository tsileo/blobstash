package main

import (
	"fmt"
	"io/ioutil"

	"github.com/bitly/go-simplejson"

	"github.com/tsileo/datadatabase/server"
	"github.com/tsileo/datadatabase/config"
	)

func main() {
	stop := make(chan bool)

	dat, err := ioutil.ReadFile("config.json")
	if err != nil {
		panic(fmt.Errorf("can't read config file: %v", err))
	}
	conf, err := simplejson.NewJson(dat)
	if err != nil {
		panic(fmt.Errorf("can't decode config file: %v", err))
	}
	_, exists := conf.CheckGet("backends")
	if !exists {
		panic(fmt.Errorf("missing top-level key \"backends\" from config file"))
	}
	blobBackend := config.NewFromConfig(conf.GetPath("backends", "blobs"))
	metaBackend := config.NewFromConfig(conf.GetPath("backends", "meta"))
	server.New("127.0.0.1:9736", "./tmp_db", blobBackend, metaBackend, stop)
}
