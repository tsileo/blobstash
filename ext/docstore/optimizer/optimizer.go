package optimizer

import (
	log "gopkg.in/inconshreveable/log15.v2"
	logext "gopkg.in/inconshreveable/log15.v2/ext"
)

var (
	Linear string = "LINEAR"
	Index  string = "INDEX"
)

type Optimizer struct {
	log     log.Logger
	indexes map[string]struct{}
}

func New(logger log.Logger) *Optimizer {
	log := logger.New("id", logext.RandId(8))
	return &Optimizer{
		log:     log,
		indexes: map[string]struct{}{},
	}
}

func (o *Optimizer) Select(q map[string]interface{}) string {
	return Linear
}
