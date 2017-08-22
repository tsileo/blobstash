package api // import "a4.io/blobstash/pkg/kvstore/api"

import (
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"

	"github.com/gorilla/mux"

	"a4.io/blobstash/pkg/ctxutil"
	"a4.io/blobstash/pkg/httputil"
	"a4.io/blobstash/pkg/stash/store"
	"a4.io/blobstash/pkg/vkv"
)

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
			ctx := ctxutil.WithNamespace(r.Context(), r.Header.Get(ctxutil.NamespaceHeader))
			q := httputil.NewQuery(r.URL.Query())
			start := q.GetDefault("cursor", "")
			limit, err := q.GetIntDefault("limit", 50)
			if err != nil {
				panic(err)
			}
			keys := []*keyValue{}
			rawKeys, cursor, err := kv.kv.Keys(ctx, start, "\xff", limit)
			if err != nil {
				panic(err)
			}
			for _, kv := range rawKeys {
				keys = append(keys, toKeyValue(kv))
			}
			srw := httputil.NewSnappyResponseWriter(w, r)
			httputil.WriteJSON(srw, map[string]interface{}{
				"data": keys,
				"pagination": map[string]interface{}{
					"cursor":   cursor,
					"has_more": len(keys) == limit,
					"count":    len(keys),
					"per_page": limit,
				},
			})
			srw.Close()

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
			ctx := ctxutil.WithNamespace(r.Context(), r.Header.Get(ctxutil.NamespaceHeader))
			limit, err := q.GetIntDefault("limit", 50)
			if err != nil {
				panic(err)
			}
			start, err := q.GetIntDefault("cursor", -1)
			if err != nil {
				panic(err)
			}
			var out []*keyValue
			resp, cursor, err := kv.kv.Versions(ctx, key, start, limit)
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
				"data": out,
				"pagination": map[string]interface{}{
					"cursor":   cursor,
					"has_more": len(out) == limit,
					"count":    len(out),
					"per_page": limit,
				},
			})
			srw.Close()
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
			ctx := ctxutil.WithNamespace(r.Context(), r.Header.Get(ctxutil.NamespaceHeader))

			q := httputil.NewQuery(r.URL.Query())
			version, err := q.GetIntDefault("version", -1)
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
			w.WriteHeader(http.StatusOK)
			if r.Method == "GET" {
				srw := httputil.NewSnappyResponseWriter(w, r)
				httputil.WriteJSON(srw, toKeyValue(item))
				srw.Close()
			}
			return
		case "POST", "PUT":
			ctx := ctxutil.WithNamespace(r.Context(), r.Header.Get(ctxutil.NamespaceHeader))

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
			res, err := kv.kv.Put(ctx, key, ref, []byte(data), version)
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

func (kv *KvStoreAPI) Register(r *mux.Router, basicAuth func(http.Handler) http.Handler) {
	r.Handle("/keys", basicAuth(http.HandlerFunc(kv.keysHandler())))
	r.Handle("/key/{key}", basicAuth(http.HandlerFunc(kv.getHandler())))
	r.Handle("/key/{key}/_versions", basicAuth(http.HandlerFunc(kv.versionsHandler())))
}
