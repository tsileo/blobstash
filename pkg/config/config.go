package config // import "a4.io/blobstash/pkg/config"

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/inconshreveable/log15"
	"gopkg.in/yaml.v2"

	"a4.io/blobstash/pkg/config/pathutil"
)

var (
	DefaultListen  = ":8051"
	LetsEncryptDir = "letsencrypt"
)

// AppConfig holds an app configuration items
type AppConfig struct {
	Name       string `yaml:"name"`
	Path       string `yaml:"path"` // App path, optional?
	Entrypoint string `yaml:"entrypoint"`
	Domain     string `yaml:"domain"`
	Username   string `yaml:"username"`
	Password   string `yaml:"password"`
	Proxy      string `yaml:"proxy"`
	Remote     string `yaml:"remote"`
	Scheduled  string `yaml:"scheduled"`

	Config map[string]interface{} `yaml:"config"`
}

type S3Repl struct {
	Bucket    string `yaml:"bucket"`
	Region    string `yaml:"region"`
	KeyFile   string `yaml:"key_file"`
	Endpoint  string `yaml:"endpoint"`
	AccessKey string `yaml:"access_key_id"`
	SecretKey string `yaml:"secret_access_key"`
}

type Replication struct {
	EnableOplog bool `yaml:"enable_oplog"`
}

type ReplicateFrom struct {
	URL    string `yaml:"url"`
	APIKey string `yaml:"api_key"`
}

func (s3 *S3Repl) Key() (*[32]byte, error) {
	if s3.KeyFile == "" {
		return nil, nil
	}
	var out [32]byte
	data, err := ioutil.ReadFile(s3.KeyFile)
	if err != nil {
		return nil, err
	}
	copy(out[:], data)
	return &out, nil
}

type BasicAuth struct {
	ID       string   `yaml:"id"`
	Roles    []string `yaml:"roles"`
	Username string   `yaml:"username"`
	Password string   `yaml:"password"`
}

type Role struct {
	Name     string                 `yaml:"name"`
	Template string                 `yaml:"template"`
	Perms    []*Perm                `yaml:"permissions'`
	Args     map[string]interface{} `yaml:"args"`

	// Only set pragmatically for "managed role"
	Managed      bool     `yaml:"-"`
	ArgsRequired []string `yaml:"-"`
}

type Perm struct {
	Action   string `yaml:"action"`
	Resource string `yaml:"resource"`
}

// Config holds the configuration items
type Config struct {
	init     bool
	Listen   string `yaml:"listen"`
	LogLevel string `yaml:"log_level"`
	// TLS     bool     `yaml:"tls"`
	AutoTLS bool     `yaml:"tls_auto"`
	Domains []string `yaml:"tls_domains"`

	Roles []*Role `yaml:"roles"`
	Auth  []*BasicAuth

	ExpvarListen string `yaml:"expvar_server_listen"`

	ExtraApacheCombinedLogs string `yaml:"extra_apache_combined_logs"`

	SharingKey string  `yaml:"sharing_key"`
	DataDir    string  `yaml:"data_dir"`
	S3Repl     *S3Repl `yaml:"s3_replication"`

	Apps          []*AppConfig    `yaml:"apps"`
	Docstore      *DocstoreConfig `yaml:"docstore"`
	Replication   *Replication    `yaml:"replication"`
	ReplicateFrom *ReplicateFrom  `yaml:"replicate_from"`

	// Items defined with the CLI flags
	ScanMode      bool `yaml:"-"`
	S3ScanMode    bool `yaml:"-"`
	S3RestoreMode bool `yaml:"-"`
}

func (c *Config) LogLvl() log15.Lvl {
	if c.LogLevel == "" {
		c.LogLevel = "info"
	}
	lvl, err := log15.LvlFromString(c.LogLevel)
	if err != nil {
		panic(err)
	}
	return lvl
}

type DocstoreConfig struct {
	StoredQueries []*StoredQuery               `yaml:"stored_queries"`
	Hooks         map[string]map[string]string `yaml:"hooks"`
}

type StoredQuery struct {
	Name string `yaml:"name"`
	Path string `yaml:"path"`
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

// VarDir returns the directory where the index will be stored
func (c *Config) StashDir() string {
	return filepath.Join(c.VarDir(), "stash")
}

// Init initialize the config.
//
// It will try to create all the needed directory.
func (c *Config) Init() error {
	if c.init {
		return nil
	}
	if _, err := os.Stat(c.VarDir()); os.IsNotExist(err) {
		if err := os.MkdirAll(c.VarDir(), 0700); err != nil {
			return err
		}
	}
	if _, err := os.Stat(c.StashDir()); os.IsNotExist(err) {
		if err := os.MkdirAll(c.VarDir(), 0700); err != nil {
			return err
		}
	}
	if _, err := os.Stat(c.ConfigDir()); os.IsNotExist(err) {
		if err := os.MkdirAll(c.ConfigDir(), 0700); err != nil {
			return err
		}
	}
	if _, err := os.Stat(filepath.Join(c.ConfigDir(), LetsEncryptDir)); os.IsNotExist(err) {
		if err := os.MkdirAll(filepath.Join(c.ConfigDir(), LetsEncryptDir), 0700); err != nil {
			return err
		}
	}
	if c.SharingKey == "" {
		return fmt.Errorf("missing `sharing_key` config item")
	}
	if c.S3Repl != nil {
		// Set default region
		if c.S3Repl.Region == "" {
			c.S3Repl.Region = "us-east-1"
		}
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
