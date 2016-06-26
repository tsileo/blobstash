package config

import (
	"fmt"
	"io/ioutil"
	"os"

	"gopkg.in/yaml.v2"

	"github.com/tsileo/blobstash/pkg/config/pathutil"
)

var (
	DefaultListen = ":8051"
)

type Config struct {
	init       bool
	Listen     string `yaml:"listen"`
	APIKey     string `yaml:"api_key"`
	SharingKey string `yaml:"sharing_key"`
	DataDir    string `yaml:"data_dir"`
}

func New(path string) (*Config, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	conf := &Config{}
	if err := yaml.Unmarshal([]byte(data), &conf); err != nil {
		return nil, err
	}
	return conf, nil
}

func (c *Config) VarDir() string {
	if c.DataDir != "" {
		return c.DataDir
	}
	return pathutil.VarDir()
}

func (c *Config) Init() error {
	if c.init {
		return nil
	}
	if _, err := os.Stat(c.VarDir()); os.IsNotExist(err) {
		if err := os.MkdirAll(c.VarDir(), 0644); err != nil {
			return err
		}
	}
	if c.SharingKey == "" {
		return fmt.Errorf("missing `sharing_key` config item")
	}
	c.init = true
	return nil
}

// Sync url config parsing
//u, err := url.Parse("http://:123@127.0.0.1:8053")
//	if err != nil {
//		log.Fatal(err)
//	}
//	u.User = nil
//	//apiKey, _ := u.User.Password()
//	fmt.Printf("%+v", u)
