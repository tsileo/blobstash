package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"log"
	"path/filepath"
	"strings"
	"runtime"
	"encoding/json"

	"github.com/bitly/go-simplejson"
	"github.com/codegangsta/cli"

	"github.com/tsileo/blobstash/db"
	"github.com/tsileo/blobstash/config"
	"github.com/tsileo/blobstash/config/pathutil"
	"github.com/tsileo/blobstash/backend"
	"github.com/tsileo/blobstash/server"
)

var version = "0.1.0"

func main() {
	app := cli.NewApp()
	app.Name = "blobstash"
	app.Version = version
	app.Usage = ""
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

var defaultConf = map[string]interface{}{
	"backends": map[string]interface{}{
		"blobs": map[string]interface{}{
			"backend-type": "blobsfile",
			"backend-args": map[string]interface{}{
				"path": "blobs",
			},
		},
	},
	"router": []interface{}{[]string{"default", "blobs"}},
}

func start(config_path string) {
	log.Printf("Starting blobstash version %v; %v (%v/%v)", version, runtime.Version(), runtime.GOOS, runtime.GOARCH)
	if config_path == "" {
		config_path = filepath.Join(pathutil.ConfigDir(), "server-config.json")
	}
	stop := make(chan bool)
	if _, err := os.Stat(config_path); os.IsNotExist(err) {
		log.Println("No config file found")
		cpath, _ := filepath.Split(config_path)
		os.MkdirAll(cpath, 0700)
		js, _ := json.MarshalIndent(&defaultConf, "", "	")
		if err := ioutil.WriteFile(config_path, js, 0644); err != nil {
			panic(fmt.Errorf("failed to create config file at %v: %v", config_path, err))
		}
		log.Printf("Config file created at %v", config_path)
	}
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
	server.New(conf.Get("listen").MustString(":9735"), conf.Get("web-listen").MustString(":9736"), conf.MustString("blobdb_db"), blobRouter,stop)
}
