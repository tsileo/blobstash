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
	"github.com/tsileo/blobstash/config/pathutil"
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
    		path = ""
    	} else {
    		path = args[0]
    	}
    	start(path)
  	}
  	app.Run(os.Args)
}

func start(config_path string) {
	if config_path == "" {
		config_path = filepath.Join(pathutil.ConfigDir(), "server-config.json")
	}
	stop := make(chan bool)
	dat, err := ioutil.ReadFile(config_path)
	if err != nil {
		panic(fmt.Errorf("failed to read config file: %v", err))
	}
	conf, err := simplejson.NewJson(dat)
	if err != nil {
		panic(fmt.Errorf("failed decode config file (invalid json): %v", err))
	}
	_, exists := conf.CheckGet("backends")
	if !exists {
		panic(fmt.Errorf("missing top-level key \"backends\" from config file"))
	}
	dbPath := pathutil.VarDir()
	os.MkdirAll(dbPath, 0700)
	routerDB, err := db.New(filepath.Join(dbPath, "index"))
	if err != nil {
		panic(err)
	}
	_, exists = conf.CheckGet("router")
	if !exists {
		panic(fmt.Errorf("missing top-level key \"router\" from config file"))
	}
	blobRouter, err := backend.NewRouterFromConfig(conf.Get("router"), routerDB)
	if err != nil {
		panic(err)
	}
	// TODO => config backend in BlobRouter
	for _, backendKey := range blobRouter.ResolveBackends() {
		cbackend := config.NewFromConfig(conf.GetPath("backends", backendKey))
		blobRouter.Backends[backendKey] = cbackend
		db, err := db.New(filepath.Join(dbPath, strings.Replace(cbackend.String(), "/", "_", -1)))
		if err != nil {
			panic(err)
		}
		blobRouter.DBs[backendKey] = db
		blobRouter.TxManagers[backendKey] = backend.NewTxManager(blobRouter.Index, db, cbackend)
	}
	err = blobRouter.Load()
	if err != nil {
		panic(err)
	}
	server.New(conf.Get("listen").MustString(":9735"), conf.Get("web-listen").MustString(":9736"), conf.MustString("blobdb_db"), blobRouter,stop)
}
