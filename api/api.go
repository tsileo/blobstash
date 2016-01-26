/*

 */
package api

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/dchest/blake2b"
	"github.com/gorilla/mux"
	"github.com/janberktold/sse"
	"github.com/tsileo/blobstash/httputil"
	serverMiddleware "github.com/tsileo/blobstash/middleware"
	"github.com/tsileo/blobstash/nsdb"
	"github.com/tsileo/blobstash/router"
	"github.com/tsileo/blobstash/vkv"
	"github.com/tsileo/blobstash/vkv/hub"
)

func vkvHandler(wg sync.WaitGroup, db *vkv.DB, kvUpdate chan *vkv.KeyValue, blobrouter *router.Router) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		switch r.Method {
		case "GET":
			iversion := -1
			version := r.URL.Query().Get("version")
			if version != "" {
				iver, err := strconv.Atoi(version)
				if err != nil {
					httputil.WriteJSONError(w, http.StatusInternalServerError, "version must be a integer")
					return
				}
				iversion = iver
			}
			res, err := db.Get(vars["key"], iversion)
			if err != nil {
				if err == vkv.ErrNotFound {
					httputil.WriteJSONError(w, http.StatusNotFound, http.StatusText(http.StatusNotFound))
					return
				}
				httputil.Error(w, err)
				return
			}
			srw := httputil.NewSnappyResponseWriter(w, r)
			httputil.WriteJSON(srw, res)
			srw.Close()
		case "HEAD":
			exists, err := db.Check(vars["key"])
			if err != nil {
				httputil.Error(w, err)
				return
			}
			if exists {
				return
			}
			httputil.WriteJSONError(w, http.StatusNotFound, http.StatusText(http.StatusNotFound))
			return
		// case "DELETE":
		// 	k := vars["key"]
		// 	sversion := r.URL.Query().Get("version")
		// 	if sversion == "" {
		// 		http.Error(w, "version missing", 500)
		// 		return
		// 	}
		// 	version, err := strconv.Atoi(sversion)
		// 	if err != nil {
		// 		http.Error(w, "bad version", 500)
		// 		return
		// 	}
		// 	hash, err := db.MetaBlob(k, version)
		// 	if err != nil {
		// 		panic(err)
		// 	}
		// 	if err := db.DeleteVersion(k, version); err != nil {
		// 		panic(err)
		// 	}
		// 	req := &router.Request{
		// 		Type:     router.Read,
		// 		MetaBlob: true,
		// 	}
		// 	if err := blobrouter.Route(req).Delete(hash); err != nil {
		// 		http.Error(w, err.Error(), http.StatusInternalServerError)
		// 	}
		case "PUT":
			wg.Add(1)
			defer wg.Done()
			k := vars["key"]
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
			res, err := db.Put(k, v, version)
			res.SetNamespace(r.Header.Get("BlobStash-Namespace"))
			if err != nil {
				panic(err)
			}
			kvUpdate <- res
			srw := httputil.NewSnappyResponseWriter(w, r)
			httputil.WriteJSON(srw, res)
			srw.Close()
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}
}

func vkvVersionsHandler(db *vkv.DB) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "GET":
			// XXX(tsileo): cursor instead of start/end?
			var err error
			q := r.URL.Query()
			limit := 0
			if q.Get("limit") != "" {
				ilimit, err := strconv.Atoi(q.Get("limit"))
				if err != nil {
					httputil.WriteJSONError(w, http.StatusInternalServerError, "limit must be an integer")
					return
				}
				limit = ilimit
			}
			start := 0
			if sstart := q.Get("start"); sstart != "" {
				start, err = strconv.Atoi(sstart)
				if err != nil {
					httputil.WriteJSONError(w, http.StatusInternalServerError, "start must be an integer")
					return
				}
			}
			end := int(time.Now().UTC().UnixNano())
			if send := q.Get("end"); send != "" {
				end, err = strconv.Atoi(send)
				if err != nil {
					httputil.WriteJSONError(w, http.StatusInternalServerError, "end must be an integer")
					return
				}
			}
			vars := mux.Vars(r)
			res, err := db.Versions(vars["key"], start, end, limit)
			if err != nil {
				httputil.Error(w, err)
				return
			}
			srw := httputil.NewSnappyResponseWriter(w, r)
			httputil.WriteJSON(srw, res)
			srw.Close()
			return
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}
}

func vkvKeysHandler(db *vkv.DB) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "GET":
			q := r.URL.Query()
			end := q.Get("end")
			if end == "" {
				end = "\xff"
			}
			limit := 0
			if q.Get("limit") != "" {
				ilimit, err := strconv.Atoi(q.Get("limit"))
				if err != nil {
					httputil.WriteJSONError(w, http.StatusInternalServerError, "limit must be an integer")
					return
				}
				limit = ilimit
			}
			res, err := db.Keys(q.Get("start"), end, limit)
			if err != nil {
				httputil.Error(w, err)
				return
			}
			srw := httputil.NewSnappyResponseWriter(w, r)
			httputil.WriteJSON(srw, map[string]interface{}{"keys": res})
			srw.Close()
			return
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}
}

func blobUploadHandler(blobs chan<- *router.Blob) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		//POST takes the uploaded file(s) and saves it to disk.
		case "POST":
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
				chash := fmt.Sprintf("%x", blake2b.Sum256(blob))
				if hash != chash {
					httputil.WriteJSONError(w, http.StatusInternalServerError, "blob corrupted, hash does not match")
					return
				}
				req := &router.Request{
					Type:      router.Write,
					Namespace: r.Header.Get("BlobStash-Namespace"),
				}
				blobs <- &router.Blob{Hash: hash, Req: req, Blob: blob}
			}
			// XXX(tsileo): returns a `http.StatusNoContent` here?
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}
}

func blobHandler(blobrouter *router.Router) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		req := &router.Request{
			Type:      router.Read,
			Namespace: r.Header.Get("BlobStash-Namespace"),
		}
		backend := blobrouter.Route(req)
		switch r.Method {
		case "GET":
			blob, err := backend.Get(vars["hash"])
			if err != nil {
				httputil.Error(w, err)
				return
			}
			srw := httputil.NewSnappyResponseWriter(w, r)
			srw.Write(blob)
			srw.Close()
			return
		case "HEAD":
			exists, err := backend.Exists(vars["hash"])
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			if exists {
				return
			}
			// XXX(tsileo): returns a `http.StatusNoContent` ?
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

func blobsHandler(blobrouter *router.Router) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		req := &router.Request{
			Type:      router.Read,
			Namespace: r.Header.Get("BlobStash-Namespace"),
		}
		backend := blobrouter.Route(req)
		switch r.Method {
		case "GET":
			srw := httputil.NewSnappyResponseWriter(w, r)
			defer srw.Close()
			// FIXME(tsileo): Re-implement this!
			//start := r.URL.Query().Get("start")
			//end := r.URL.Query().Get("end")
			//slimit := r.URL.Query().Get("limit")
			//limit := 0
			//if slimit != "" {
			//	lim, err := strconv.Atoi(slimit)
			//	if err != nil {
			//		panic(err)
			//	}
			//	limit = lim
			//}
			blobs := make(chan string)
			errc := make(chan error, 1)
			go func() {
				errc <- backend.Enumerate(blobs)
			}()
			res := []string{}
			for blob := range blobs {
				res = append(res, blob)
			}
			if err := <-errc; err != nil {
				httputil.Error(w, err)
				return
			}
			httputil.WriteJSON(srw, map[string]interface{}{"blobs": res})
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}
}

func vkvWatchKeyHandler(vkvhub *hub.Hub) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		defer func() {
			log.Printf("WATCH CLOSE")
		}()
		conn, _ := sse.Upgrade(w, r)
		notify := w.(http.CloseNotifier).CloseNotify()
		stream := vkvhub.Sub(vars["key"])
	L:
		for {
			select {
			case <-notify:
				log.Printf("Close")
				vkvhub.Unsub(vars["key"], stream)
				break L
			case m := <-stream:
				conn.WriteString(m)
			}
		}
	}
}
func New(r *mux.Router, middlewares *serverMiddleware.SharedMiddleware, wg sync.WaitGroup, db *vkv.DB, ns *nsdb.DB,
	kvUpdate chan *vkv.KeyValue, blobrouter *router.Router, blobs chan<- *router.Blob, vkvHub *hub.Hub) {

	r.Handle("/blobstore/upload", middlewares.Auth(http.HandlerFunc(blobUploadHandler(blobs))))
	r.Handle("/blobstore/blobs", middlewares.Auth(http.HandlerFunc(blobsHandler(blobrouter))))
	r.Handle("/blobstore/blob/{hash}", middlewares.Auth(http.HandlerFunc(blobHandler(blobrouter))))
	r.Handle("/vkv/keys", middlewares.Auth(http.HandlerFunc(vkvKeysHandler(db))))
	r.Handle("/vkv/key/{key}", middlewares.Auth(http.HandlerFunc(vkvHandler(wg, db, kvUpdate, blobrouter))))
	r.Handle("/vkv/key/{key}/versions", middlewares.Auth(http.HandlerFunc(vkvVersionsHandler(db))))
	// XXX(tsileo); is the watch SSE endpoint really needed?
	r.Handle("/vkv/key/{key}/watch", middlewares.Auth(http.HandlerFunc(vkvWatchKeyHandler(vkvHub))))
}
