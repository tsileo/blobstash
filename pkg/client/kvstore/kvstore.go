package kvstore // import "a4.io/blobstash/pkg/client/kvstore"

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"a4.io/blobstash/pkg/client/clientutil"
	"a4.io/blobstash/pkg/client/response"
)

var defaultServerAddr = "http://localhost:8050"
var defaultUserAgent = "KvStore Go client v1"

var ErrKeyNotFound = errors.New("key doest not exist")

type KvStore struct {
	client *clientutil.Client
}

func DefaultOpts() *clientutil.Opts {
	return &clientutil.Opts{
		SnappyCompression: false,
		Host:              defaultServerAddr,
		UserAgent:         defaultUserAgent,
		APIKey:            "",
	}
}

func New(opts *clientutil.Opts) *KvStore {
	if opts == nil {
		opts = DefaultOpts()
	}
	return &KvStore{
		client: clientutil.New(opts),
	}
}

func (kvs *KvStore) Client() *clientutil.Client {
	return kvs.client
}

func (kvs *KvStore) Put(ctx context.Context, key, ref string, pdata []byte, version int) (*response.KeyValue, error) {
	data := url.Values{}
	data.Set("data", string(pdata))
	data.Set("ref", ref)
	if version != -1 {
		data.Set("version", strconv.Itoa(version))
	}
	resp, err := kvs.client.DoReq(ctx, "PUT", "/api/kvstore/key/"+key, nil, strings.NewReader(data.Encode())) //data.Encode()))
	if err != nil {
		return nil, err
	}
	var body bytes.Buffer
	body.ReadFrom(resp.Body)
	resp.Body.Close()
	switch resp.StatusCode {
	case 200:
		kv := &response.KeyValue{}
		if err := json.Unmarshal(body.Bytes(), kv); err != nil {
			return nil, err
		}
		return kv, nil
	default:
		return nil, fmt.Errorf("failed to put key %v: %v", key, body.String())
	}
}

func (kvs *KvStore) Get(ctx context.Context, key string, version int) (*response.KeyValue, error) {
	resp, err := kvs.client.DoReq(ctx, "GET", fmt.Sprintf("/api/kvstore/key/%s?version=%v", key, version), nil, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	switch {
	case resp.StatusCode == 200:
		kv := &response.KeyValue{}
		if err := json.Unmarshal(body, kv); err != nil {
			return nil, err
		}
		return kv, nil
	case resp.StatusCode == 404:
		return nil, ErrKeyNotFound
	default:
		return nil, fmt.Errorf("failed to get key %v: %v", key, string(body))
	}

}

func (kvs *KvStore) Versions(ctx context.Context, key string, start, end, limit int) (*response.KeyValueVersions, error) {
	// TODO handle start, end and limit
	resp, err := kvs.client.DoReq(ctx, "GET", "/api/kvstore/key/"+key+fmt.Sprintf("/_versions?start=%d&end=%d&limit=%d", start, end, limit), nil, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	switch {
	case resp.StatusCode == 200:
		kvversions := &response.KeyValueVersions{}
		if err := json.Unmarshal(body, kvversions); err != nil {
			return nil, err
		}
		return kvversions, nil
	case resp.StatusCode == 404:
		return nil, ErrKeyNotFound
	default:
		return nil, fmt.Errorf("failed to get key %v: %v", key, string(body))
	}
}

// nextKey returns the next key for lexigraphical (key = NextKey(lastkey))
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

func (kvs *KvStore) Keys(ctx context.Context, prefix, start, end string, limit int) ([]*response.KeyValue, error) {
	resp, err := kvs.client.DoReq(ctx, "GET", fmt.Sprintf("/api/kvstore/keys?prefix=%s&start=%s&end=%s&limit=%d", prefix, start, end, limit), nil, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	switch {
	case resp.StatusCode == 200:
		keys := &response.KeysResponse{}
		if err := json.Unmarshal(body, keys); err != nil {
			return nil, err
		}
		return keys.Keys, nil
	default:
		return nil, fmt.Errorf("failed to get keys: %v", string(body))
	}
}

type KvStore2 struct {
	client *clientutil.ClientUtil
}

func New2(c *clientutil.ClientUtil) *KvStore2 {
	return &KvStore2{c}
}

func (kvs *KvStore2) Put(ctx context.Context, key, ref string, pdata []byte, version int) (*response.KeyValue, error) {
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

func (kvs *KvStore2) Get(ctx context.Context, key string, version int) (*response.KeyValue, error) {
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

func (kvs *KvStore2) Versions(ctx context.Context, key string, start, end, limit int) (*response.KeyValueVersions, error) {
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

func (kvs *KvStore2) Keys(ctx context.Context, prefix, start, end string, limit int) ([]*response.KeyValue, error) {
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
