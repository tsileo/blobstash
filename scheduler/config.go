package scheduler

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"time"
)

var (
	config        *Config
	configLock    = new(sync.RWMutex)
	configUpdated = make(chan struct{})
)

func loadConfig(fail bool) {
	file, err := ioutil.ReadFile("config.json")
	if err != nil {
		log.Println("open config: ", err)
		if fail {
			os.Exit(1)
		}
	}

	temp := new(Config)
	if err = json.Unmarshal(file, temp); err != nil {
		log.Println("parse config: ", err)
		if fail {
			os.Exit(1)
		}
	}
	configLock.Lock()
	config = temp
	configLock.Unlock()
}

func GetConfig() *Config {
	configLock.RLock()
	defer configLock.RUnlock()
	return config
}

func watchFile(filePath string) error {
	initialStat, err := os.Stat(filePath)
	if err != nil {
		return err
	}

	for {
		stat, err := os.Stat(filePath)
		if err != nil {
			return err
		}

		if stat.Size() != initialStat.Size() || stat.ModTime() != initialStat.ModTime() {
			break
		}

		time.Sleep(1 * time.Second)
	}

	return nil
}

func init() {
	loadConfig(true)
	go func() {
		for {
			err := watchFile("config.json")
			if err != nil {
				fmt.Println(err)
			}
			loadConfig(false)
			configUpdated <- struct{}{}
		}
	}()
}

type ConfigEntry struct {
	Path string `json:"path"`
	Spec string `json:"spec"`
}

type Config struct {
	AnacronMode bool          `json:"anacron_mode"`
	Snapshots   []ConfigEntry `json:"snapshots"`
}
