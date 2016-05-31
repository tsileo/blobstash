package config

import (
	"io/ioutil"

	"gopkg.in/yaml.v2"
)

type Config struct {
	Listen string `yaml:"listen"`
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
