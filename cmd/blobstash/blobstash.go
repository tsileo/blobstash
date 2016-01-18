package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"runtime"

	"github.com/codegangsta/cli"

	"github.com/tsileo/blobstash/config/pathutil"
	"github.com/tsileo/blobstash/logger"
	"github.com/tsileo/blobstash/server"
)

func main() {
	app2 := cli.NewApp()
	app2.Name = "blobstash"
	app2.Version = server.Version
	app2.Usage = ""
	app2.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "loglevel",
			Value: "info",
		},
		cli.StringFlag{
			Name: "resync",
		},
		cli.IntFlag{
			Name:  "port",
			Value: 8050,
		},
	}
	app2.Action = func(c *cli.Context) {
		var path string
		args := c.Args()
		if len(args) == 0 {
			path = ""
		} else {
			path = args[0]
		}
		start(path, c.String("loglevel"), c.Bool("resync"), c.Int("port"))
	}
	app2.Run(os.Args)
}

func start(config_path, loglevel string, resync bool, port int) {
	logger.InitLogger(loglevel)
	l := logger.Log
	l.Info(fmt.Sprintf("Starting blobstash version %v; %v (%v/%v)", server.Version, runtime.Version(), runtime.GOOS, runtime.GOARCH))
	if config_path == "" {
		config_path = filepath.Join(pathutil.ConfigDir(), "server-config.json")
	}
	if _, err := os.Stat(config_path); os.IsNotExist(err) {
		log.Println("No config file found")
		cpath, _ := filepath.Split(config_path)
		os.MkdirAll(cpath, 0700)
		js, _ := json.MarshalIndent(&server.DefaultConf, "", "	")
		if err := ioutil.WriteFile(config_path, js, 0644); err != nil {
			panic(fmt.Errorf("failed to create config file at %v: %v", config_path, err))
		}
		log.Printf("Config file created at %v", config_path)
	}
	dat, err := ioutil.ReadFile(config_path)
	if err != nil {
		panic(fmt.Errorf("failed to read config file: %v", err))
	}
	// FIXME(tsileo): don't override port if it's present in a config file
	var conf map[string]interface{}
	if err := json.Unmarshal(dat, &conf); err != nil {
		panic(fmt.Errorf("failed decode config file (invalid json): %v", err))
	}
	conf["resync"] = resync
	conf["port"] = port
	s := server.New(conf)
	s.Run()
}
