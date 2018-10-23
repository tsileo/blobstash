package main

import (
	"fmt"
	"io/ioutil"
	"os"

	yaml "gopkg.in/yaml.v2"
)

// RemoteConfig holds the "remote endpoint" configuration
type RemoteConfig struct {
	Endpoint        string `yaml:"endpoint"`
	Region          string `yaml:"region"`
	Bucket          string `yaml:"bucket"`
	AccessKeyID     string `yaml:"access_key_id"`
	SecretAccessKey string `yaml:"secret_access_key"`
	KeyFile         string `yaml:"key_file"`
}

// Profile holds a profile configuration
type Profile struct {
	RemoteConfig *RemoteConfig `yaml:"remote_config"`
	Endpoint     string        `yaml:"endpoint"`
	APIKey       string        `yaml:"api_key"`
}

// Config holds config profiles
type Config map[string]*Profile

// loadProfile loads the config file and the given profile within it
func loadProfile(configFile, name string) (*Profile, error) {
	dat, err := ioutil.ReadFile(configFile)
	switch {
	case err == nil:
	case os.IsNotExist(err):
		return nil, nil
	default:
		return nil, err
	}
	out := Config{}
	if err := yaml.Unmarshal(dat, out); err != nil {
		return nil, err
	}

	prof, ok := out[name]
	if !ok {
		return nil, fmt.Errorf("profile %s not found", name)
	}

	return prof, nil
}
