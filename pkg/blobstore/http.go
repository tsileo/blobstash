package blobstore

import (
	"bytes"
	"io"
	"net/http"

	"github.com/gorilla/mux"
	"golang.org/x/net/context"

	mblob "a4.io/blobstash/pkg/blob"
	"a4.io/blobstash/pkg/client/clientutil"
	"a4.io/blobstash/pkg/ctxutil"
	"a4.io/blobstash/pkg/hashutil"
	"a4.io/blobstash/pkg/httputil"
)

func (bs *BlobStore) Register(r *mux.Router, basicAuth func(http.Handler) http.Handler) {
	r.Handle("/blobs", basicAuth(http.HandlerFunc(bs.enumerateHandler())))
	r.Handle("/upload", basicAuth(http.HandlerFunc(bs.uploadHandler())))
	r.Handle("/blob/{hash}", basicAuth(http.HandlerFunc(bs.blobHandler())))
}

func (bs *BlobStore) uploadHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "GET":
			// FIXME(tsileo): download the content from r.URL.Query().Get("url") and upload it, returns its ref
		//POST takes the uploaded file(s) and saves it to disk.
		case "POST":
			ctx := ctxutil.WithRequest(context.Background(), r)

			if ns := r.Header.Get("BlobStash-Namespace"); ns != "" {
				ctx = ctxutil.WithNamespace(ctx, ns)
			}

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
				chash := hashutil.Compute(blob)
				if hash != chash {
					httputil.WriteJSONError(w, http.StatusInternalServerError, "blob corrupted, hash does not match, expected "+chash)
					return
				}
				b := &mblob.Blob{Hash: hash, Data: blob}
				if err := bs.Put(ctx, b); err != nil {
					httputil.WriteJSONError(w, http.StatusInternalServerError, err.Error())
				}
			}
			// XXX(tsileo): returns a `http.StatusNoContent` here?
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}
}

func (bs *BlobStore) blobHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := ctxutil.WithRequest(context.Background(), r)

		if ns := r.Header.Get("BlobStash-Namespace"); ns != "" {
			ctx = ctxutil.WithNamespace(ctx, ns)
		}
		vars := mux.Vars(r)
		switch r.Method {
		case "GET":
			blob, err := bs.Get(ctx, vars["hash"])
			if err != nil {
				if err == clientutil.ErrBlobNotFound {
					httputil.WriteJSONError(w, http.StatusNotFound, http.StatusText(http.StatusNotFound))
				} else {
					httputil.Error(w, err)
				}
				return
			}
			srw := httputil.NewSnappyResponseWriter(w, r)
			srw.Write(blob)
			srw.Close()
			return
		case "HEAD":
			exists, err := bs.Stat(ctx, vars["hash"])
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			if exists {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			httputil.WriteJSONError(w, http.StatusNotFound, http.StatusText(http.StatusNotFound))
			return
		// case "DELETE":
		// 	if err := backend.Delete(vars["hash"]); err != nil {
		// 		http.Error(w, err.Error(), http.StatusInternalServerError)
		// 	}
		// 	return
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}
}

func (bs *BlobStore) enumerateHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := ctxutil.WithRequest(context.Background(), r)

		if ns := r.Header.Get("BlobStash-Namespace"); ns != "" {
			ctx = ctxutil.WithNamespace(ctx, ns)
		}
		switch r.Method {
		case "GET":
			// end := r.URL.Query().Get("end")
			// if end == "" {
			// 	end = "\xff"
			// }
			// var scan bool
			// if sscan := r.URL.Query().Get("scan"); sscan != "" {
			// 	scan = true
			// }
			end := "\xff"
			// TODO(tsileo): parse limit and set default to 0
			refs, err := bs.Enumerate(ctx, r.URL.Query().Get("start"), end, 0)
			if err != nil {
				httputil.Error(w, err)
				return
			}
			srw := httputil.NewSnappyResponseWriter(w, r)
			httputil.WriteJSON(srw, map[string]interface{}{"refs": refs})
			srw.Close()
			return
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}
}
