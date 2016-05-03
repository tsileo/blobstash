package kvstore

import (
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
	return "kv"
}

func (km *KvMeta) Dump() []byte {
	return []byte{}
}

func New(logger log.Logger, blobStore *blobstore.BlobStore, metaHandler *meta.Meta) (*KvStore, error) {
	logger.Debug("init")
	// TODO(tsileo): handle config
	kv, err := vkv.New("/Users/thomas/var/blobstash/vkv")
	if err != nil {
		return nil, err
	}
	return &KvStore{
		blobStore: blobStore,
		meta:      metaHandler,
		log:       logger,
		vkv:       kv,
	}, nil
}

func (kv *KvStore) getHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		key := mux.Vars(r)["key"]
		switch r.Method {
		//POST takes the uploaded file(s) and saves it to disk.
		case "GET, HEAD":
			ctx := ctxutil.WithRequest(context.Background(), r)

			if ns := r.Header.Get("BlobStash-Namespace"); ns != "" {
				ctx = ctxutil.WithNamespace(ctx, ns)
			}

			item, err := kv.vkv.Get(key, -1)
			if err != nil {
				panic(err)
			}
			// TODO(tsileo): handle HEAD
			srw := httputil.NewSnappyResponseWriter(w, r)
			httputil.WriteJSON(srw, item)
			srw.Close()
			return
		case "POST":
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
			res, err := kv.vkv.Put(key, v, version)
			kvmeta := NewKvMeta(res)
			if err := kv.meta.Save(kvmeta); err != nil {
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
	r.Handle("/key/{}", http.HandlerFunc(kv.getHandler()))
}
