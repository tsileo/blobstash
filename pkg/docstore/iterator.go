package docstore

import (
	"context"
	"fmt"

	"a4.io/blobstash/pkg/docstore/id"
	"a4.io/blobstash/pkg/stash/store"
)

// IDIterator is the interface that wraps the Iter method
type IDIterator interface {
	Iter(collection string, cursor string, fetchLimit int, asOf int64) (ids []*id.ID, nextCursor string, err error)
}

// noIndexIterator is the default iterator that will return document sorted by insert data (descending order, most recent first)
type noIndexIterator struct {
	kvStore store.KvStore
}

func newNoIndexIterator(kvStore store.KvStore) *noIndexIterator {
	return &noIndexIterator{
		kvStore: kvStore,
	}
}

// Iter implements the IDIterator interface
func (i *noIndexIterator) Iter(collection, cursor string, fetchLimit int, asOf int64) ([]*id.ID, string, error) {
	// TODO(tsileo): check if a cursor > ID(asOf) => repace it with ID(asOf) as an optimization
	// start = fmt.Sprintf(keyFmt, collection, cursor)
	end := fmt.Sprintf(keyFmt, collection, "")
	_ids := []*id.ID{}
	res, nextCursor, err := i.kvStore.ReverseKeys(context.TODO(), end, cursor, fetchLimit)
	if err != nil {
		return nil, "", err
	}
	for _, kv := range res {
		// Build the ID
		_id, err := idFromKey(collection, kv.Key)
		if err != nil {
			return nil, "", err
		}

		// Add the extra metadata to the ID
		_id.SetFlag(kv.Data[0])
		_id.SetVersion(kv.Version)

		// Skip deleted document
		if _id.Flag() == flagDeleted {
			continue
		}

		// Add the ID
		_ids = append(_ids, _id)
	}
	return _ids, nextCursor, nil
}
