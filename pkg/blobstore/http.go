package blobstore

import (
	"bytes"
	"io"
	"net/http"

	"github.com/gorilla/mux"
	"golang.org/x/net/context"

	"github.com/tsileo/blobstash/client/clientutil"
	"github.com/tsileo/blobstash/httputil"
	mblob "github.com/tsileo/blobstash/pkg/blob"
	"github.com/tsileo/blobstash/pkg/ctxutil"
	"github.com/tsileo/blobstash/pkg/hashutil"
	"github.com/tsileo/blobstash/pkg/middleware"
)

func (bs *BlobStore) uploadHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
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

func (bs *BlobStore) Register(r *mux.Router) {
	r.Handle("/blobs", middleware.BasicAuth(http.HandlerFunc(bs.enumerateHandler()), bs.conf))
	r.Handle("/upload", middleware.BasicAuth(http.HandlerFunc(bs.uploadHandler()), bs.conf))
	r.Handle("/blob/{hash}", middleware.BasicAuth(http.HandlerFunc(bs.blobHandler()), bs.conf))
}
