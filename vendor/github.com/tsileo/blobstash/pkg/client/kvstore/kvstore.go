package kvstore // import "a4.io/blobstash/pkg/client/kvstore"

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"

	"a4.io/blobstash/pkg/client/clientutil"
	"a4.io/blobstash/pkg/client/response"
)

var ErrKeyNotFound = errors.New("key doest not exist")

func nextKey(key string) string {
	bkey := []byte(key)
	i := len(bkey)
	for i > 0 {
		i--
		bkey[i]++
		if bkey[i] != 0 {
			break
		}
	}
	return string(bkey)
}

type KvStore struct {
	client *clientutil.ClientUtil
}

func New(c *clientutil.ClientUtil) *KvStore {
	return &KvStore{c}
}

func (kvs *KvStore) Put(ctx context.Context, key, ref string, pdata []byte, version int) (*response.KeyValue, error) {
	data := url.Values{}
	data.Set("data", string(pdata))
	data.Set("ref", ref)
	if version != -1 {
		data.Set("version", strconv.Itoa(version))
	}
	resp, err := kvs.client.Post("/api/kvstore/key/"+key, []byte(data.Encode()))
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if err := clientutil.ExpectStatusCode(resp, http.StatusOK); err != nil {
		return nil, err
	}

	// TODO(tsileo): better place than response?
	kv := &response.KeyValue{}
	if err := clientutil.Unmarshal(resp, kv); err != nil {
		return nil, err
	}

	return kv, nil
}

func (kvs *KvStore) Get(ctx context.Context, key string, version int) (*response.KeyValue, error) {
	resp, err := kvs.client.Get(fmt.Sprintf("/api/kvstore/key/%s?version=%v", key, version))
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if err := clientutil.ExpectStatusCode(resp, 200); err != nil {
		if err.IsNotFound() {
			return nil, ErrKeyNotFound
		}
		return nil, err
	}

	kv := &response.KeyValue{}
	if err := clientutil.Unmarshal(resp, kv); err != nil {
		return nil, err
	}

	return kv, nil
}

func (kvs *KvStore) Versions(ctx context.Context, key string, start, end, limit int) (*response.KeyValueVersions, error) {
	// TODO handle start, end and limit
	resp, err := kvs.client.Get(fmt.Sprintf("/api/kvstore/key/%s/_versions?start=%d&end=%d&limit=%d", key, start, end, limit))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if err := clientutil.ExpectStatusCode(resp, http.StatusOK); err != nil {
		if err.IsNotFound() {
			return nil, ErrKeyNotFound
		}
		return nil, err
	}

	kvversions := &response.KeyValueVersions{}
	if err := clientutil.Unmarshal(resp, kvversions); err != nil {
		return nil, err
	}
	return kvversions, nil
}

func (kvs *KvStore) Keys(ctx context.Context, prefix, start, end string, limit int) ([]*response.KeyValue, error) {
	resp, err := kvs.client.Get(fmt.Sprintf("/api/kvstore/keys?prefix=%s&start=%s&end=%s&limit=%d", prefix, start, end, limit))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if err := clientutil.ExpectStatusCode(resp, http.StatusOK); err != nil {
		return nil, err
	}

	keys := &response.KeysResponse{}
	if err := clientutil.Unmarshal(resp, keys); err != nil {
		return nil, err
	}

	return keys.Keys, nil
}
