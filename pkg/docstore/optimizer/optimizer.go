package optimizer

import (
	"fmt"

	"github.com/tsileo/blobstash/pkg/docstore/index"
	"github.com/tsileo/blobstash/pkg/docstore/maputil"

	log "gopkg.in/inconshreveable/log15.v2"
	logext "gopkg.in/inconshreveable/log15.v2/ext"
)

var (
	Linear string = "LINEAR"
	Index  string = "INDEX"
)

type Optimizer struct {
	log     log.Logger
	indexes map[string]*index.Index
}

func New(logger log.Logger, indexes []*index.Index) *Optimizer {
	log := logger.New("id", logext.RandId(8))
	indexesMap := map[string]*index.Index{}
	for _, idx := range indexes {
		indexesMap[idx.ID] = idx
	}
	return &Optimizer{
		log:     log,
		indexes: indexesMap,
	}
}

func (o *Optimizer) Select(q map[string]interface{}) (string, *index.Index) {
	// FIXME(tsileo): returns a slice of *index.Index
	if q != nil && len(q) == 1 {
		fields := []string{}
		for field, _ := range q {
			fields = append(fields, field)
		}
		if idx, ok := o.indexes[fmt.Sprintf("single-field-%s", fields[0])]; ok {
			o.log.Info("found index for query", "optimizer", Index, "index", idx.ID)
			return Index, idx
		}
	}
	o.log.Info("selected optimizer", "optimizer", Linear)
	return Linear, nil
}

func (o *Optimizer) ShouldIndex(doc map[string]interface{}) (bool, *index.Index, string) {
	// XXX(tsileo): should Select returns a index?
	fdoc := maputil.FlattenMap(doc, "", ".")
	for k, _ := range fdoc {
		if idx, ok := o.indexes[fmt.Sprintf("single-field-%s", k)]; ok {
			val, err := maputil.GetPath(k, doc)
			if err != nil {
				panic(err)
			}
			idxKey := index.IndexKey(val)
			return true, idx, idxKey
		}
	}
	return false, nil, ""
}
