package main

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/bitly/go-simplejson"
	"github.com/codegangsta/cli"

	"github.com/tsileo/blobstash/config"
	"github.com/tsileo/blobstash/server"
)

func main() {
	app := cli.NewApp()
	app.Name = "BlobDB"
	app.Usage = "BlobMachine database/CAS server"
	app.Action = func(c *cli.Context) {
    	var path string
    	args := c.Args()
    	if len(args) == 0 {
    		path = "config.json"
    	} else {
    		path = args[0]
    	}
    	start(path)
  	}
  	app.Run(os.Args)
}

func start(config_path string) {
	stop := make(chan bool)

	dat, err := ioutil.ReadFile(config_path)
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
