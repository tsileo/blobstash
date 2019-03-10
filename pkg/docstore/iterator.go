package docstore

import (
	"context"
	"fmt"
	"strconv"

	"a4.io/blobstash/pkg/docstore/id"
	"a4.io/blobstash/pkg/stash/store"
	"a4.io/blobstash/pkg/vkv"
)

// IDIterator is the interface that wraps the Iter method
//
// Iter allow to iterates over all the valid document IDs for a given asOf (as <= 0 means "as of now")
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
	end := fmt.Sprintf(keyFmt, collection, "")
	asOfStr := strconv.FormatInt(asOf, 10)
	_ids := []*id.ID{}

	// List keys from the kvstore
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

		if asOf > 0 && _id.Ts() > asOf {
			// Skip documents created after the requested asOf
			continue
		}

		// Add the extra metadata to the ID
		_id.SetFlag(kv.Data[0])
		_id.SetVersion(kv.Version)

		// Skip deleted document
		if _id.Flag() == flagDeleted {
			continue
		}

		if asOf <= 0 {
			// Add the current ID as no
			_ids = append(_ids, _id)
		} else {
			// A specific asOf is requested
			if _id.Ts() == _id.Version() {
				// If the document has only one version, and it's anterior to the requested asOf, we select the doc
				_ids = append(_ids, _id)
			} else {
				// Check if the document has a valid version for the given asOf
				kvv, _, err := i.kvStore.Versions(context.TODO(), fmt.Sprintf(keyFmt, collection, _id.String()), asOfStr, 1)
				if err != nil {
					if err == vkv.ErrNotFound {
						continue
					}
					return nil, "", err
				}

				// No anterior versions, skip it
				if len(kvv.Versions) == 0 {
					continue
				}

				// Update the ID metadata (to let the query engine fetch the right document version immediately)
				kv = kvv.Versions[0]
				_id.SetFlag(kv.Data[0])
				_id.SetVersion(kv.Version)

				// Sanity check
				if _id.Flag() == flagDeleted {
					continue
				}

				// Select the doc
				_ids = append(_ids, _id)

			}
		}
	}
	return _ids, nextCursor, nil
}
