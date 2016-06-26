package config

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v2"

	"github.com/tsileo/blobstash/pkg/config/pathutil"
)

var (
	DefaultListen  = ":8051"
	LetsEncryptDir = "letsencrypt"
)

// Config holds the configuration items
type Config struct {
	init   bool
	Listen string `yaml:"listen"`
	// TLS     bool     `yaml:"tls"`
	AutoTLS bool     `yaml:"tls_auto"`
	Domains []string `yaml:"tls_domains"`

	APIKey     string `yaml:"api_key"`
	SharingKey string `yaml:"sharing_key"`
	DataDir    string `yaml:"data_dir"`
}

// New initialize a config object by loading the YAML path at the given path
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

// VarDir returns the directory where the index will be stored
func (c *Config) ConfigDir() string {
	// TODO(tsileo): allow override?
	return pathutil.ConfigDir()
}

// VarDir returns the directory where the index will be stored
func (c *Config) VarDir() string {
	if c.DataDir != "" {
		return c.DataDir
	}
	return pathutil.VarDir()
}

// Init intialize the config.
//
// It will try to create all the needed directory.
func (c *Config) Init() error {
	if c.init {
		return nil
	}
	if _, err := os.Stat(c.VarDir()); os.IsNotExist(err) {
		if err := os.MkdirAll(c.VarDir(), 0644); err != nil {
			return err
		}
	}
	if _, err := os.Stat(c.ConfigDir()); os.IsNotExist(err) {
		if err := os.MkdirAll(c.ConfigDir(), 0644); err != nil {
			return err
		}
	}
	if _, err := os.Stat(filepath.Join(c.ConfigDir(), LetsEncryptDir)); os.IsNotExist(err) {
		if err := os.MkdirAll(filepath.Join(c.ConfigDir(), LetsEncryptDir), 0644); err != nil {
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
