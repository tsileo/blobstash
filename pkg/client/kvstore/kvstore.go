package kvstore // import "a4.io/blobstash/pkg/client/kvstore"

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
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
		EnableHTTP2:       true,
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

func (kvs *KvStore) Put(key, ref string, pdata []byte, version int) (*response.KeyValue, error) {
	data := url.Values{}
	data.Set("data", string(pdata))
	data.Set("ref", ref)
	if version != -1 {
		data.Set("version", strconv.Itoa(version))
	}
	resp, err := kvs.client.DoReq("PUT", "/api/kvstore/key/"+key, nil, strings.NewReader(data.Encode())) //data.Encode()))
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

func (kvs *KvStore) Get(key string, version int) (*response.KeyValue, error) {
	resp, err := kvs.client.DoReq("GET", fmt.Sprintf("/api/kvstore/key/%s?version=%v", key, version), nil, nil)
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

func (kvs *KvStore) Versions(key string, start, end, limit int) (*response.KeyValueVersions, error) {
	// TODO handle start, end and limit
	resp, err := kvs.client.DoReq("GET", "/api/kvstore/key/"+key+fmt.Sprintf("/_versions?start=%d&end=%d&limit=%d", start, end, limit), nil, nil)
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

func (kvs *KvStore) Keys(start, end string, limit int) ([]*response.KeyValue, error) {
	resp, err := kvs.client.DoReq("GET", fmt.Sprintf("/api/kvstore/keys?start=%v&end=%v&limit=%d", start, end, limit), nil, nil)
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
