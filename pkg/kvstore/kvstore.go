package kvstore // import "a4.io/blobstash/pkg/kvstore"

import (
	"fmt"
	"github.com/gorilla/mux"
	log "github.com/inconshreveable/log15"
	"golang.org/x/net/context"
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
	applied, err := kv.vkv.MetaBlobApplied(hash)
	if err != nil {
		return err
	}
	if !applied {
		kv.log.Debug("meta not yet applied")
		rkv, err := vkv.UnserializeBlob(data)
		if err != nil {
			return fmt.Errorf("failed to unserialize blob: %v", err)
		}
		if _, err := kv.Put(context.Background(), rkv.Key, rkv.Hash, rkv.Data, rkv.Version); err != nil {
			return fmt.Errorf("failed to put: %v", err)
		}
		kv.log.Debug("Applied meta", "kv", rkv)
	}
	return nil
}

func (kv *KvStore) Close() error {
	return kv.vkv.Close()
}

func (kv *KvStore) Get(ctx context.Context, key string, version int) (*vkv.KeyValue, error) {
	_, fromHttp := ctxutil.Request(ctx)
	kv.log.Info("OP Get", "from_http", fromHttp, "key", key, "version", version)
	return kv.vkv.Get(key, -1)
}

func (kv *KvStore) Keys(ctx context.Context, start, end string, limit int) ([]*vkv.KeyValue, error) {
	_, fromHttp := ctxutil.Request(ctx)
	kv.log.Info("OP Keys", "from_http", fromHttp)
	return kv.vkv.Keys(start, end, limit)
}

func (kv *KvStore) Versions(ctx context.Context, key string, start, end, limit int) (*vkv.KeyValueVersions, error) {
	_, fromHttp := ctxutil.Request(ctx)
	kv.log.Info("OP Versions", "from_http", fromHttp, "key", key, "start", start, "end", end)
	// FIXME(tsileo): decide between -1/0 for default, or introduce a constant Max/Min?? and the end only make sense for the reverse Versions?
	if end == -1 {
		end = int(time.Now().UTC().UnixNano())
	}
	return kv.vkv.Versions(key, start, end, 0)
}

func (kv *KvStore) ReversePrefixKeys(prefix, start, end string, limit int) ([]*vkv.KeyValue, error) {
	return kv.vkv.ReversePrefixKeys(prefix, start, end, limit)
}

func (kv *KvStore) PutPrefix(ctx context.Context, prefix, key, ref string, data []byte, version int) (*vkv.KeyValue, error) {
	res, err := kv.vkv.PutPrefix(prefix, key, ref, data, version)
	// TODO(tsileo): cleanup/DRY the Put/PutPrefix
	metaBlob, err := kv.meta.Build(res)
	if err != nil {
		return nil, err
	}
	if err := res.SetMetaBlob(metaBlob.Hash); err != nil {
		return nil, err
	}
	if err := kv.blobStore.Put(ctx, metaBlob); err != nil {
		return nil, err
	}
	return res, nil

}

func (kv *KvStore) Put(ctx context.Context, key, ref string, data []byte, version int) (*vkv.KeyValue, error) {
	// _, fromHttp := ctxutil.Request(ctx)
	// kv.log.Info("OP Put", "from_http", fromHttp, "key", key, "value", value, "version", version)
	res, err := kv.vkv.Put(key, ref, data, version)
	if err != nil {
		return nil, err
	}
	metaBlob, err := kv.meta.Build(res)
	if err != nil {
		return nil, err
	}
	if err := res.SetMetaBlob(metaBlob.Hash); err != nil {
		return nil, err
	}
	if err := kv.blobStore.Put(ctx, metaBlob); err != nil {
		return nil, err
	}
	return res, nil
}

func (kv *KvStore) versionsHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		key := mux.Vars(r)["key"]
		switch r.Method {
		//POST takes the uploaded file(s) and saves it to disk.
		case "GET", "HEAD":
			ctx := ctxutil.WithRequest(context.Background(), r)

			if ns := r.Header.Get("BlobStash-Namespace"); ns != "" {
				ctx = ctxutil.WithNamespace(ctx, ns)
			}
			limit := 0
			start := 0
			end := -1
			for _, q := range []struct {
				v    *int
				def  int
				name string
			}{
				{&limit, 0, "limit"},
				{&start, 0, "start"},
				{&end, -1, "end"},
			} {
				var err error
				val := q.def
				if svalue := r.URL.Query().Get(q.name); svalue != "" {
					val, err = strconv.Atoi(svalue)
					if err != nil {
						panic(err)
					}
				}
				*q.v = val
			}
			resp, err := kv.Versions(ctx, key, start, end, 0)
			if err != nil {
				if err == vkv.ErrNotFound {
					w.WriteHeader(http.StatusNotFound)
					w.Write([]byte(http.StatusText(http.StatusNotFound)))
					return
				}
				panic(err)
			}
			// TODO(tsileo): handle HEAD
			srw := httputil.NewSnappyResponseWriter(w, r)
			httputil.WriteJSON(srw, resp)
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
		//POST takes the uploaded file(s) and saves it to disk.
		case "GET", "HEAD":
			ctx := ctxutil.WithRequest(context.Background(), r)

			if ns := r.Header.Get("BlobStash-Namespace"); ns != "" {
				ctx = ctxutil.WithNamespace(ctx, ns)
			}

			// FIXME(tsileo): read the version from query args
			item, err := kv.Get(ctx, key, -1)
			if err != nil {
				if err == vkv.ErrNotFound {
					w.WriteHeader(http.StatusNotFound)
					w.Write([]byte(http.StatusText(http.StatusNotFound)))
					return
				}
				panic(err)
			}
			// TODO(tsileo): handle HEAD
			srw := httputil.NewSnappyResponseWriter(w, r)
			httputil.WriteJSON(srw, item)
			srw.Close()
			return
		case "POST", "PUT":
			ctx := ctxutil.WithRequest(context.Background(), r)

			if ns := r.Header.Get("BlobStash-Namespace"); ns != "" {
				ctx = ctxutil.WithNamespace(ctx, ns)
			}

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
			httputil.WriteJSON(srw, res)
			srw.Close()
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}
}

// if err != nil {
// 	if err == clientutil.ErrBlobNotFound {
// 		httputil.WriteJSONError(w, http.StatusNotFound, http.StatusText(http.StatusNotFound))
// 	} else {
// 		httputil.Error(w, err)
// 	}
// 	return
// }
// srw := httputil.NewSnappyResponseWriter(w, r)
// srw.Write(blob)
// srw.Close()
// return
// case "HEAD":
// exists, err := bs.Stat(ctx, vars["hash"])
// if err != nil {
// 	http.Error(w, err.Error(), http.StatusInternalServerError)
// }
// if exists {
// 	w.WriteHeader(http.StatusNoContent)
// 	return
// }
// httputil.WriteJSONError(w, http.StatusNotFound, http.StatusText(http.StatusNotFound))
// return
// // case "DELETE":
// // 	if err := backend.Delete(vars["hash"]); err != nil {
// // 		http.Error(w, err.Error(), http.StatusInternalServerError)
// // 	}
// // 	return
// default:
// w.WriteHeader(http.StatusMethodNotAllowed)
// }
// }
// }

func (kv *KvStore) Register(r *mux.Router, basicAuth func(http.Handler) http.Handler) {
	r.Handle("/key/{key}", basicAuth(http.HandlerFunc(kv.getHandler())))
	r.Handle("/key/{key}/_versions", basicAuth(http.HandlerFunc(kv.versionsHandler())))
}
