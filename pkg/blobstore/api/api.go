package api // import "a4.io/blobstash/pkg/blobstore/api"

import (
	"bytes"
	"io"
	"net/http"

	"github.com/gorilla/mux"

	"a4.io/blobsfile"
	mblob "a4.io/blobstash/pkg/blob"
	"a4.io/blobstash/pkg/ctxutil"
	"a4.io/blobstash/pkg/hashutil"
	"a4.io/blobstash/pkg/httputil"
	"a4.io/blobstash/pkg/stash/store"
)

type BlobStoreAPI struct {
	bs store.BlobStore
}

func New(bs store.BlobStore) *BlobStoreAPI {
	return &BlobStoreAPI{bs}
}

func (bs *BlobStoreAPI) Register(r *mux.Router, basicAuth func(http.Handler) http.Handler) {
	r.Handle("/blobs", basicAuth(http.HandlerFunc(bs.enumerateHandler())))
	r.Handle("/upload", basicAuth(http.HandlerFunc(bs.uploadHandler())))
	r.Handle("/blob/{hash}", basicAuth(http.HandlerFunc(bs.blobHandler())))
}

func (bs *BlobStoreAPI) uploadHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "GET":
			// FIXME(tsileo): download the content from r.URL.Query().Get("url") and upload it, returns its ref
		//POST takes the uploaded file(s) and saves it to disk.
		case "POST":
			ctx := ctxutil.WithNamespace(r.Context(), r.Header.Get(ctxutil.NamespaceHeader))

			//parse the multipart form in the request
			mr, err := r.MultipartReader()
			if err != nil {
				httputil.Error(w, err)
				return
			}

			for {
				part, err := mr.NextPart()
				if err == io.EOF {
					break
				}
				if err != nil {
					httputil.Error(w, err)
					return
				}
				hash := part.FormName()
				var buf bytes.Buffer
				buf.ReadFrom(part)
				blob := buf.Bytes()
				// FIXME(tsileo): should we do the check here? or let the storage engine do it
				chash := hashutil.Compute(blob)
				if hash != chash {
					httputil.WriteJSONError(w, http.StatusInternalServerError, "blob corrupted, hash does not match, expected "+chash)
					return
				}
				b := &mblob.Blob{Hash: hash, Data: blob}
				if err := bs.bs.Put(ctx, b); err != nil {
					httputil.WriteJSONError(w, http.StatusInternalServerError, err.Error())
				}
			}
			// XXX(tsileo): returns a `http.StatusNoContent` here?
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}
}

func (bs *BlobStoreAPI) blobHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := ctxutil.WithNamespace(r.Context(), r.Header.Get(ctxutil.NamespaceHeader))
		vars := mux.Vars(r)
		switch r.Method {
		case "GET":
			// FIXME(tsileo): clean this case, skip a decoding/encoding round and return the bytes as is from the
			// backend storage
			if r.Header.Get("Accept-Encoding") == "snappy" {
				blob, err := bs.bs.GetEncoded(ctx, vars["hash"])
				if err != nil {
					if err == blobsfile.ErrBlobNotFound {
						httputil.WriteJSONError(w, http.StatusNotFound, http.StatusText(http.StatusNotFound))
					} else {
						httputil.Error(w, err)
					}
					return
				}
				httputil.WriteEncoded(r, w, blob)
				return
			}

			blob, err := bs.bs.Get(ctx, vars["hash"])
			if err != nil {
				if err == blobsfile.ErrBlobNotFound {
					httputil.WriteJSONError(w, http.StatusNotFound, http.StatusText(http.StatusNotFound))
				} else {
					httputil.Error(w, err)
				}
				return
			}
			httputil.Write(r, w, blob)
			return
		case "HEAD":
			exists, err := bs.bs.Stat(ctx, vars["hash"])
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			if exists {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			httputil.WriteJSONError(w, http.StatusNotFound, http.StatusText(http.StatusNotFound))
			return
		case "POST":
			blob, err := httputil.Read(r)
			if err != nil {
				httputil.Error(w, err)
				return
			}

			// FIXME(tsileo): should we do the check here? or let the storage engine do it
			// XXX(tsileo): if the blob is already snappy encoded, find a way to skip the extra decoding/encoding like for GET
			chash := hashutil.Compute(blob)
			if vars["hash"] != chash {
				httputil.WriteJSONError(w, http.StatusInternalServerError, "blob corrupted, hash does not match, expected "+chash)
				return
			}

			b := &mblob.Blob{Hash: vars["hash"], Data: blob}
			if err := bs.bs.Put(ctx, b); err != nil {
				httputil.WriteJSONError(w, http.StatusInternalServerError, err.Error())
			}

			w.WriteHeader(http.StatusCreated)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}
}

func (bs *BlobStoreAPI) enumerateHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "GET":
			ctx := ctxutil.WithNamespace(r.Context(), r.Header.Get(ctxutil.NamespaceHeader))
			q := httputil.NewQuery(r.URL.Query())
			limit, err := q.GetInt("limit", 50, 1000)
			if err != nil {
				httputil.Error(w, err)
				return
			}
			// var scan bool
			// if sscan := r.URL.Query().Get("scan"); sscan != "" {
			// 	scan = true
			// }
			refs, nextCursor, err := bs.bs.Enumerate(ctx, q.Get("cursor"), "\xff", limit)
			if err != nil {
				httputil.Error(w, err)
				return
			}

			httputil.MarshalAndWrite(r, w, map[string]interface{}{
				"data": refs,
				"pagination": map[string]interface{}{
					"cursor":   nextCursor,
					"has_more": len(refs) == limit,
					"count":    len(refs),
					"per_page": limit,
				},
			})
			return
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}
}
