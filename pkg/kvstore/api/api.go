package api // import "a4.io/blobstash/pkg/kvstore/api"

import (
	"net/http"
	"net/url"

	"github.com/gorilla/mux"

	"a4.io/blobstash/pkg/auth"
	"a4.io/blobstash/pkg/ctxutil"
	"a4.io/blobstash/pkg/httputil"
	"a4.io/blobstash/pkg/perms"
	"a4.io/blobstash/pkg/stash/store"
	"a4.io/blobstash/pkg/vkv"
)

type keyValue struct {
	Key     string `json:"key"`
	Version int64  `json:"version"`
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

type KvStoreAPI struct {
	kv store.KvStore
}

func New(kv store.KvStore) *KvStoreAPI {
	return &KvStoreAPI{kv}
}

func (kv *KvStoreAPI) keysHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "GET":
			if !auth.Can(
				w,
				r,
				perms.Action(perms.List, perms.KVEntry),
				perms.Resource(perms.KvStore, perms.KVEntry),
			) {
				auth.Forbidden(w)
				return
			}

			ctx := ctxutil.WithNamespace(r.Context(), r.Header.Get(ctxutil.NamespaceHeader))
			q := httputil.NewQuery(r.URL.Query())
			start := q.GetDefault("cursor", "")
			limit, err := q.GetIntDefault("limit", 50)
			if err != nil {
				panic(err)
			}
			reverse, err := q.GetBoolDefault("reverse", false)
			if err != nil {
				panic(err)
			}
			keys := []*keyValue{}
			var rawKeys []*vkv.KeyValue
			var cursor string
			if reverse {
				rawKeys, cursor, err = kv.kv.ReverseKeys(ctx, start, "\xff", limit)
			} else {
				rawKeys, cursor, err = kv.kv.Keys(ctx, start, "\xff", limit)
			}
			if err != nil {
				panic(err)
			}

			for _, kv := range rawKeys {
				keys = append(keys, toKeyValue(kv))
			}
			httputil.MarshalAndWrite(r, w, map[string]interface{}{
				"data": keys,
				"pagination": map[string]interface{}{
					"cursor":   cursor,
					"has_more": len(keys) == limit,
					"count":    len(keys),
					"per_page": limit,
				},
			})

		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}
}

func (kv *KvStoreAPI) versionsHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		key := mux.Vars(r)["key"]
		q := httputil.NewQuery(r.URL.Query())
		switch r.Method {
		//POST takes the uploaded file(s) and saves it to disk.
		case "GET", "HEAD":
			if !auth.Can(
				w,
				r,
				perms.Action(perms.Read, perms.KVEntry),
				perms.ResourceWithID(perms.KvStore, perms.KVEntry, key),
			) {
				auth.Forbidden(w)
				return
			}

			ctx := ctxutil.WithNamespace(r.Context(), r.Header.Get(ctxutil.NamespaceHeader))
			limit, err := q.GetIntDefault("limit", 50)
			if err != nil {
				panic(err)
			}
			start := q.GetDefault("cursor", "0")
			var out []*keyValue
			resp, cursor, err := kv.kv.Versions(ctx, key, start, limit)
			if err != nil {
				if err == vkv.ErrNotFound {
					w.WriteHeader(http.StatusNotFound)
					w.Write([]byte(http.StatusText(http.StatusNotFound)))
					return
				}
				panic(err)
			}
			for _, v := range resp.Versions {
				out = append(out, toKeyValue(v))
			}
			httputil.MarshalAndWrite(r, w, map[string]interface{}{
				"data": out,
				"pagination": map[string]interface{}{
					"cursor":   cursor,
					"has_more": cursor != "0" && len(out) == limit,
					"count":    len(out),
					"per_page": limit,
				},
			})
			return
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}
}

func (kv *KvStoreAPI) getHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		key := mux.Vars(r)["key"]
		switch r.Method {
		case "GET", "HEAD":
			if !auth.Can(
				w,
				r,
				perms.Action(perms.Read, perms.KVEntry),
				perms.ResourceWithID(perms.KvStore, perms.KVEntry, key),
			) {
				auth.Forbidden(w)
				return
			}

			ctx := ctxutil.WithNamespace(r.Context(), r.Header.Get(ctxutil.NamespaceHeader))

			q := httputil.NewQuery(r.URL.Query())
			version, err := q.GetInt64Default("version", -1)
			if err != nil {
				panic(err)
			}

			item, err := kv.kv.Get(ctx, key, version)
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
			if r.Method == "GET" {
				httputil.MarshalAndWrite(r, w, toKeyValue(item))
			}
			w.WriteHeader(http.StatusOK)
			return
		case "POST", "PUT":
			if !auth.Can(
				w,
				r,
				perms.Action(perms.Write, perms.KVEntry),
				perms.ResourceWithID(perms.KvStore, perms.KVEntry, key),
			) {
				auth.Forbidden(w)
				return
			}

			ctx := ctxutil.WithNamespace(r.Context(), r.Header.Get(ctxutil.NamespaceHeader))

			// Parse the form value
			hah, err := httputil.Read(r)
			values, err := url.ParseQuery(string(hah))
			if err != nil {
				httputil.Error(w, err)
				return
			}
			q := httputil.NewQuery(values)
			ref := values.Get("ref")
			data := values.Get("data")
			version, err := q.GetInt64Default("version", -1)
			if err != nil {
				httputil.Error(w, err)
				return
			}
			res, err := kv.kv.Put(ctx, key, ref, []byte(data), version)
			if err != nil {
				httputil.Error(w, err)
				return
			}
			httputil.MarshalAndWrite(r, w, toKeyValue(res))
			// TODO(tsileo): switch to StatusCreated
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}
}

func (kv *KvStoreAPI) Register(r *mux.Router, basicAuth func(http.Handler) http.Handler) {
	r.Handle("/keys", basicAuth(http.HandlerFunc(kv.keysHandler())))
	r.Handle("/key/{key}", basicAuth(http.HandlerFunc(kv.getHandler())))
	r.Handle("/key/{key}/_versions", basicAuth(http.HandlerFunc(kv.versionsHandler())))
}
