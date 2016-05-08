package kvstore

import (
	"encoding/json"
	"github.com/gorilla/mux"
	log "github.com/inconshreveable/log15"
	"golang.org/x/net/context"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"

	_ "github.com/tsileo/blobstash/backend"
	"github.com/tsileo/blobstash/httputil"
	"github.com/tsileo/blobstash/pkg/blobstore"
	"github.com/tsileo/blobstash/pkg/ctxutil"
	"github.com/tsileo/blobstash/pkg/meta"
	"github.com/tsileo/blobstash/vkv"
)

const KvType = "kv"

// FIXME(tsileo): take a ctx as first arg for each method

type KvStore struct {
	blobStore *blobstore.BlobStore
	meta      *meta.Meta
	log       log.Logger

	vkv *vkv.DB
}

type KvMeta struct {
	kv *vkv.KeyValue
}

func NewKvMeta(kv *vkv.KeyValue) *KvMeta {
	return &KvMeta{kv: kv}
}

func (km *KvMeta) Type() string {
	return KvType
}

func (km *KvMeta) Dump() ([]byte, error) {
	return json.Marshal(km.kv)
}

func New(logger log.Logger, blobStore *blobstore.BlobStore, metaHandler *meta.Meta) (*KvStore, error) {
	logger.Debug("init")
	// TODO(tsileo): handle config
	kv, err := vkv.New("/Users/thomas/var/blobstash/vkv")
	if err != nil {
		return nil, err
	}
	kvStore := &KvStore{
		blobStore: blobStore,
		meta:      metaHandler,
		log:       logger,
		vkv:       kv,
	}
	metaHandler.RegisterApplyFunc(KvType, kvStore.ApplyMetaFunc)
	return kvStore, nil
}

func (kv *KvStore) ApplyMetaFunc(data []byte) error {
	rkv := &vkv.KeyValue{}
	if err := json.Unmarshal(data, rkv); err != nil {
		return err
	}
	kv.log.Debug("Applying meta", "kv", rkv)
	// TODO(ts): apply the kv
	return nil
}

func (kv *KvStore) Get(ctx context.Context, key string, version int) (*vkv.KeyValue, error) {
	_, fromHttp := ctxutil.Request(ctx)
	kv.log.Info("OP Get", "from_http", fromHttp, "key", key, "version", version)
	return kv.vkv.Get(key, -1)
}

func (kv *KvStore) Put(ctx context.Context, key, value string, version int) (*vkv.KeyValue, error) {
	_, fromHttp := ctxutil.Request(ctx)
	kv.log.Info("OP Put", "from_http", fromHttp, "key", key, "value", value, "version", version)
	res, err := kv.vkv.Put(key, value, version)
	if err != nil {
		return nil, err
	}
	kvmeta := NewKvMeta(res)
	if err := kv.meta.Save(kvmeta); err != nil {
		return nil, err
	}
	return res, nil
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

			item, err := kv.Get(ctx, key, -1)
			if err != nil {
				panic(err)
			}
			// TODO(tsileo): handle HEAD
			srw := httputil.NewSnappyResponseWriter(w, r)
			httputil.WriteJSON(srw, item)
			srw.Close()
			return
		case "POST":
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
			v := values.Get("value")
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
			res, err := kv.Put(ctx, key, v, version)
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

func (kv *KvStore) Register(r *mux.Router) {
	r.Handle("/key/{key}", http.HandlerFunc(kv.getHandler()))
}
