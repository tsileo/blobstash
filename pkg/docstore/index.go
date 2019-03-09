package docstore

import (
	"a4.io/blobstash/pkg/docstore/id"
	"a4.io/blobstash/pkg/stash/store"
)

// Indexer is the interface that wraps the Index method
type Indexer interface {
	Index(id *id.ID, doc map[string]interface{}) error
}

type sortIndex struct {
	kvStore store.KvStore
	fields  []string
	name    string
}

func newSortIndex(name string, fields ...string) *sortIndex {
	return &sortIndex{
		name:   name,
		fields: fields,
	}
}
