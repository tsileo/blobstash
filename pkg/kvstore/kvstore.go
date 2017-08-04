package kvstore // import "a4.io/blobstash/pkg/kvstore"

import (
	"context"
	"fmt"
	"github.com/gorilla/mux"
	log "github.com/inconshreveable/log15"
	"io/ioutil"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"time"

	"a4.io/blobstash/pkg/blobstore"
	"a4.io/blobstash/pkg/config"
	"a4.io/blobstash/pkg/ctxutil"
	"a4.io/blobstash/pkg/httputil"
	"a4.io/blobstash/pkg/meta"
	"a4.io/blobstash/pkg/vkv"
)

const KvType = "kv"

// FIXME(tsileo): take a ctx as first arg for each method

type KvStore struct {
	blobStore *blobstore.BlobStore
	meta      *meta.Meta
	log       log.Logger
	conf      *config.Config

	vkv *vkv.DB
}

type keyValue struct {
	Key     string `json:"key"`
	Version int    `json:"version"`
	Hash    string `json:"hash,omitempty"`
	Data    []byte `json:"data,omitempty"`
}

func toKeyValue(okv *vkv.KeyValue) *keyValue {
	return &keyValue{
		Key:     okv.Key,
		Version: okv.Version,
		Hash:    okv.HexHash(),
		Data:    okv.Data,
	}
}

func New(logger log.Logger, conf *config.Config, blobStore *blobstore.BlobStore, metaHandler *meta.Meta) (*KvStore, error) {
	logger.Debug("init")
	// TODO(tsileo): handle config
	kv, err := vkv.New(filepath.Join(conf.VarDir(), "vkv"))
	if err != nil {
		return nil, err
	}
	kvStore := &KvStore{
		blobStore: blobStore,
		meta:      metaHandler,
		log:       logger,
		conf:      conf,
		vkv:       kv,
	}
	metaHandler.RegisterApplyFunc(KvType, kvStore.applyMetaFunc)
	return kvStore, nil
}

func (kv *KvStore) applyMetaFunc(hash string, data []byte) error {
	kv.log.Debug("Apply meta init", "hash", hash)
	// applied, err := kv.vkv.MetaBlobApplied(hash)
	// if err != nil {
	// return err
	// }
	// if !applied {
	// kv.log.Debug("meta not yet applied")
	rkv, err := vkv.UnserializeBlob(data)
	if err != nil {
		return fmt.Errorf("failed to unserialize blob: %v", err)
	}
	if _, err := kv.Put(context.Background(), rkv.Key, rkv.HexHash(), rkv.Data, rkv.Version); err != nil {
		return fmt.Errorf("failed to put: %v", err)
	}
	kv.log.Debug("Applied meta", "kv", rkv)
	// }
	return nil
}

func (kv *KvStore) Close() error {
	return kv.vkv.Close()
}

func (kv *KvStore) Get(ctx context.Context, key string, version int) (*vkv.KeyValue, error) {
	_, fromHttp := ctxutil.Request(ctx)
	kv.log.Info("OP Get", "from_http", fromHttp, "key", key, "version", version)
	return kv.vkv.Get(key, version)
}

func (kv *KvStore) Keys(ctx context.Context, start, end string, limit int) ([]*vkv.KeyValue, string, error) {
	_, fromHttp := ctxutil.Request(ctx)
	kv.log.Info("OP Keys", "from_http", fromHttp, "start", "", "end", end)
	return kv.vkv.Keys(start, end, limit)
}

func (kv *KvStore) Versions(ctx context.Context, key string, start, limit int) (*vkv.KeyValueVersions, int, error) {
	_, fromHttp := ctxutil.Request(ctx)
	kv.log.Info("OP Versions", "from_http", fromHttp, "key", key, "start", start)
	// FIXME(tsileo): decide between -1/0 for default, or introduce a constant Max/Min?? and the end only make sense for the reverse Versions?
	if start == -1 {
		start = int(time.Now().UTC().UnixNano())
	}
	res, cursor, err := kv.vkv.Versions(key, 0, start, limit)
	if err != nil {
		return nil, cursor, err
	}

	return res, cursor, nil
}

func (kv *KvStore) ReverseKeys(start, end string, limit int) ([]*vkv.KeyValue, string, error) {
	return kv.vkv.ReverseKeys(start, end, limit)
}

func (kv *KvStore) Put(ctx context.Context, key, ref string, data []byte, version int) (*vkv.KeyValue, error) {
	// _, fromHttp := ctxutil.Request(ctx)
	// kv.log.Info("OP Put", "from_http", fromHttp, "key", key, "value", value, "version", version)
	res := &vkv.KeyValue{
		Key:     key,
		Version: version,
		Data:    data,
	}
	if ref != "" {
		res.SetHexHash(ref)
	}
	if err := kv.vkv.Put(res); err != nil {
		return nil, err
	}
	metaBlob, err := kv.meta.Build(res)
	if err != nil {
		return nil, err
	}
	if err := kv.blobStore.Put(ctx, metaBlob); err != nil {
		return nil, err
	}
	return res, nil
}

func (kv *KvStore) keysHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "GET":
			// ctx := ctxutil.WithRequest(context.Background(), r)
			q := httputil.NewQuery(r.URL.Query())
			start := q.GetDefault("start", "")
			limit, err := q.GetIntDefault("limit", -1)
			if err != nil {
				panic(err)
			}
			keys := []*keyValue{}
			rawKeys, cursor, err := kv.vkv.Keys(start, "\xff", limit)
			if err != nil {
				panic(err)
			}
			for _, kv := range rawKeys {
				keys = append(keys, toKeyValue(kv))
			}
			srw := httputil.NewSnappyResponseWriter(w, r)
			httputil.WriteJSON(srw, map[string]interface{}{"keys": keys, "cursor": cursor})
			srw.Close()

		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}
}

func (kv *KvStore) versionsHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		key := mux.Vars(r)["key"]
		q := httputil.NewQuery(r.URL.Query())
		switch r.Method {
		//POST takes the uploaded file(s) and saves it to disk.
		case "GET", "HEAD":
			ctx := ctxutil.WithRequest(context.Background(), r)
			limit, err := q.GetIntDefault("limit", -1)
			if err != nil {
				panic(err)
			}
			start, err := q.GetIntDefault("cursor", -1)
			if err != nil {
				panic(err)
			}
			var out []*keyValue
			resp, cursor, err := kv.Versions(ctx, key, start, limit)
			for _, v := range resp.Versions {
				out = append(out, toKeyValue(v))
			}
			if err != nil {
				if err == vkv.ErrNotFound {
					w.WriteHeader(http.StatusNotFound)
					w.Write([]byte(http.StatusText(http.StatusNotFound)))
					return
				}
				panic(err)
			}
			srw := httputil.NewSnappyResponseWriter(w, r)
			httputil.WriteJSON(srw, map[string]interface{}{
				"key":      resp.Key,
				"versions": out,
				"cursor":   strconv.Itoa(cursor),
			})
			srw.Close()
			return
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}
}

func (kv *KvStore) getHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		key := mux.Vars(r)["key"]
		switch r.Method {
		case "GET", "HEAD":
			ctx := ctxutil.WithRequest(context.Background(), r)

			q := httputil.NewQuery(r.URL.Query())
			version, err := q.GetIntDefault("version", -1)
			if err != nil {
				panic(err)
			}

			item, err := kv.Get(ctx, key, version)
			if err != nil {
				if err == vkv.ErrNotFound {
					w.WriteHeader(http.StatusNotFound)
					if r.Method == "GET" {
						w.Write([]byte(http.StatusText(http.StatusNotFound)))
					}
					return
				}
				panic(err)
			}
			w.WriteHeader(http.StatusOK)
			if r.Method == "GET" {
				srw := httputil.NewSnappyResponseWriter(w, r)
				httputil.WriteJSON(srw, toKeyValue(item))
				srw.Close()
			}
			return
		case "POST", "PUT":
			ctx := ctxutil.WithRequest(context.Background(), r)

			// Parse the form value
			hah, err := ioutil.ReadAll(r.Body)
			values, err := url.ParseQuery(string(hah))
			if err != nil {
				httputil.Error(w, err)
				return
			}
			ref := values.Get("ref")
			data := values.Get("data")
			sversion := values.Get("version")
			version := -1
			if sversion != "" {
				iversion, err := strconv.Atoi(sversion)
				if err != nil {
					httputil.WriteJSONError(w, http.StatusInternalServerError, "version must be an integer")
					return
				}
				version = iversion
			}
			res, err := kv.Put(ctx, key, ref, []byte(data), version)
			if err != nil {
				httputil.Error(w, err)
				return
			}
			srw := httputil.NewSnappyResponseWriter(w, r)
			httputil.WriteJSON(srw, toKeyValue(res))
			srw.Close()
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}
}

func (kv *KvStore) Register(r *mux.Router, basicAuth func(http.Handler) http.Handler) {
	r.Handle("/keys", basicAuth(http.HandlerFunc(kv.keysHandler())))
	r.Handle("/key/{key}", basicAuth(http.HandlerFunc(kv.getHandler())))
	r.Handle("/key/{key}/_versions", basicAuth(http.HandlerFunc(kv.versionsHandler())))
}
