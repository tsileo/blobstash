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
	app := cli.NewApp()
	app.Name = "blobstash"
	app.Version = server.Version
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

func start(config_path string) {
	logger.InitLogger("debug")
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
	var conf map[string]interface{}
	if err := json.Unmarshal(dat, &conf); err != nil {
		panic(fmt.Errorf("failed decode config file (invalid json): %v", err))
	}
	server.New(conf).Run()
}
