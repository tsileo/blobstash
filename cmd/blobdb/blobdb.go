package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/bitly/go-simplejson"
	"github.com/codegangsta/cli"

	"github.com/tsileo/blobstash/db"
	"github.com/tsileo/blobstash/config"
	"github.com/tsileo/blobstash/backend"
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
	blobRouter, err := backend.NewRouterFromConfig(conf.Get("router"))
	if err != nil {
		panic(err)
	}
	// TODO => config backend in BlobRouter
	dbPath := conf.Get("db-path").MustString("dbs")
	os.MkdirAll(dbPath, 0700)
	for _, backendKey := range blobRouter.ResolveBackends() {
		cbackend := config.NewFromConfig(conf.GetPath("backends", backendKey))
		blobRouter.Backends[backendKey] = cbackend
		db, err := db.New(filepath.Join(dbPath, strings.Replace(cbackend.String(), "/", "_", -1)))
		if err != nil {
			panic(err)
		}
		blobRouter.DBs[backendKey] = db
		blobRouter.TxManagers[backendKey] = backend.NewTxManager(db, cbackend)
	}
	err = blobRouter.Load()
	if err != nil {
		panic(err)
	}
	server.New(conf.Get("addr").MustString(":9735"), conf.Get("web-addr").MustString(":9736"), conf.MustString("blobdb_db"), blobRouter,stop)
}
