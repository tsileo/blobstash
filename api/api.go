/*

*/
package api

import (
	"bytes"
	"encoding/json"
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
	"github.com/rs/cors"
	"github.com/tsileo/blobstash/router"
	"github.com/tsileo/blobstash/vkv"
	"github.com/tsileo/blobstash/vkv/hub"
)

func WriteJSON(w http.ResponseWriter, data interface{}) {
	js, err := json.Marshal(data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(js)
}

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
					panic(err)
				}
				iversion = iver
			}
			res, err := db.Get(vars["key"], iversion)
			if err != nil {
				if err == vkv.ErrNotFound {
					http.Error(w, http.StatusText(404), 404)
					return
				}
				panic(err)
			}
			WriteJSON(w, res)
		case "HEAD":
			exists, err := db.Check(vars["key"])
			if err != nil {
				panic(err)
			}
			if exists {
				return
			}
			http.Error(w, http.StatusText(404), 404)
			return
		case "DELETE":
			k := vars["key"]
			sversion := r.URL.Query().Get("version")
			if sversion == "" {
				http.Error(w, "version missing", 500)
				return
			}
			version, err := strconv.Atoi(sversion)
			if err != nil {
				http.Error(w, "bad version", 500)
				return
			}
			hash, err := db.MetaBlob(k, version)
			if err != nil {
				panic(err)
			}
			if err := db.DeleteVersion(k, version); err != nil {
				panic(err)
			}
			req := &router.Request{
				Type:     router.Read,
				MetaBlob: true,
			}
			if err := blobrouter.Route(req).Delete(hash); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
		case "PUT":
			wg.Add(1)
			defer wg.Done()
			k := vars["key"]
			hah, err := ioutil.ReadAll(r.Body)
			values, err := url.ParseQuery(string(hah))
			if err != nil {
				panic(err)
			}
			v := values.Get("value")
			sversion := values.Get("version")
			version := -1
			if sversion != "" {
				iversion, err := strconv.Atoi(sversion)
				if err != nil {
					http.Error(w, "bad version", 500)
					return
				}
				version = iversion
			}
			res, err := db.Put(k, v, version)
			if err != nil {
				panic(err)
			}
			kvUpdate <- res
			WriteJSON(w, res)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}
}

func vkvVersionsHandler(db *vkv.DB) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "GET":
			// TODO handle start/end/limit
			vars := mux.Vars(r)
			res, err := db.Versions(vars["key"], 0, int(time.Now().UTC().UnixNano()), 0)
			if err != nil {
				panic(err)
			}
			WriteJSON(w, res)
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
					http.Error(w, "bad limit", 500)
				}
				limit = ilimit
			}
			res, err := db.Keys(q.Get("start"), end, limit)
			if err != nil {
				panic(err)
			}
			WriteJSON(w, map[string]interface{}{"keys": res})
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
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			for {
				part, err := mr.NextPart()
				if err == io.EOF {
					break
				}
				if err != nil {
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
				hash := part.FormName()
				var buf bytes.Buffer
				buf.ReadFrom(part)
				blob := buf.Bytes()
				chash := fmt.Sprintf("%x", blake2b.Sum256(blob))
				if hash != chash {
					http.Error(w, "blob corrupted, hash does not match", http.StatusInternalServerError)
					return
				}
				req := &router.Request{
					Type: router.Write,
					//	Namespace: r.URL.Query().Get("ns"),
				}
				blobs <- &router.Blob{Hash: hash, Req: req, Blob: blob}
			}
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}
}

func blobHandler(blobrouter *router.Router) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		req := &router.Request{
			Type: router.Read,
			//	Namespace: r.URL.Query().Get("ns"),
		}
		backend := blobrouter.Route(req)
		switch r.Method {
		case "GET":
			blob, err := backend.Get(vars["hash"])
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			w.Write(blob)
			return
		case "HEAD":
			exists, err := backend.Exists(vars["hash"])
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			if exists {
				return
			}
			http.Error(w, http.StatusText(404), 404)
			return
		case "DELETE":
			if err := backend.Delete(vars["hash"]); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			return
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}
}

func blobsHandler(blobrouter *router.Router) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		req := &router.Request{
			Type: router.Read,
			//	Namespace: r.URL.Query().Get("ns"),
		}
		backend := blobrouter.Route(req)
		switch r.Method {
		case "GET":
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
				panic(err)
			}
			WriteJSON(w, map[string]interface{}{"blobs": res})
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
func New(wg sync.WaitGroup, db *vkv.DB, kvUpdate chan *vkv.KeyValue, blobrouter *router.Router, blobs chan<- *router.Blob, vkvHub *hub.Hub) http.Handler {
	r := mux.NewRouter()
	r.HandleFunc("/api/v1/blobstore/upload", blobUploadHandler(blobs))
	r.HandleFunc("/api/v1/blobstore/blobs", blobsHandler(blobrouter))
	r.HandleFunc("/api/v1/blobstore/blob/{hash}", blobHandler(blobrouter))
	r.HandleFunc("/api/v1/vkv/keys", vkvKeysHandler(db))
	r.HandleFunc("/api/v1/vkv/key/{key}", vkvHandler(wg, db, kvUpdate, blobrouter))
	r.HandleFunc("/api/v1/vkv/key/{key}/versions", vkvVersionsHandler(db))
	r.HandleFunc("/api/v1/vkv/key/{key}/watch", vkvWatchKeyHandler(vkvHub))
	// FIXME allowedorigins from config
	c := cors.New(cors.Options{
		AllowedOrigins: []string{"*"},
	})
	return c.Handler(r)
}
