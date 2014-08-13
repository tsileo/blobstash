/*
http://sanatgersappa.blogspot.fr/2013/03/handling-multiple-file-uploads-in-go.html

*/
package server

import (
	"net/http"
	"bytes"
	"io"
	"strconv"
	"encoding/json"
	"fmt"

	"github.com/gorilla/mux"

	"github.com/tsileo/blobstash/backend"
	"github.com/tsileo/blobstash/scripting"
)

func blobHandler(router *backend.Router) func(http.ResponseWriter, *http.Request) {
	return func (w http.ResponseWriter, r *http.Request) {
		meta, _ := strconv.ParseBool(r.Header.Get("BlobStash-Meta"))
		req := &backend.Request{
			Namespace: r.Header.Get("BlobStash-Namespace"),
			MetaBlob: meta,
		}
		vars := mux.Vars(r)
		switch {
		case r.Method == "HEAD":
			exists := router.Exists(req, vars["hash"])
			if exists {
				return
			}
			http.Error(w, http.StatusText(404), 404)
			return
		case r.Method == "GET":
			blob, err := router.Get(req, vars["hash"])
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			w.Write(blob)
			return
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
	}
}

func uploadHandler(jobc chan<- *blobPutJob) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {

		//POST takes the uploaded file(s) and saves it to disk.
		case "POST":
			namespace := r.Header.Get("BlobStash-Namespace")
			//ctx := r.Header.Get("BlobStash-Ctx")
			meta, _ := strconv.ParseBool(r.Header.Get("BlobStash-Meta"))

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
				breq := &backend.Request{
					Namespace: namespace,
					MetaBlob: meta,
				}
				jobc<- newBlobPutJob(breq, hash, buf.Bytes(), nil)
			}
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}
}
func WriteJSON(w http.ResponseWriter, data interface{}) {
    js, err := json.Marshal(data)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    w.Header().Set("Content-Type", "application/json")
    w.Write(js)
}
// ScriptingHandler registers the "scripting" handling.
func ScriptingHandler(jobc chan<- *blobPutJob, router *backend.Router) func(http.ResponseWriter, *http.Request) {
    return func (w http.ResponseWriter, r *http.Request) {
       switch {
        case r.Method == "POST":
            decoder := json.NewDecoder(r.Body)
            data := map[string]interface{}{}
            if err := decoder.Decode(&data); err != nil {
                http.Error(w, err.Error(), http.StatusInternalServerError)
            }
            req := &backend.Request{
                Namespace: r.Header.Get("BlobStash-Namespace"),
            }
            db := router.DB(req)
            fmt.Printf("Received script: %v\n", data)
            code := data["_script"].(string)
            sargs := data["_args"].(string)
            args := map[string]interface{}{}
            if err := json.Unmarshal([]byte(sargs), &args); err != nil {
            	http.Error(w, err.Error(), http.StatusInternalServerError)
            }
            out, tx := scripting.ExecScript(db, code, args)
            err, isErr := out["error"]
            if isErr {
				http.Error(w, err.(string), http.StatusInternalServerError)
            }
            if tx.Len() > 0 {
            	hash, js := tx.Dump()
            	jobc<- newBlobPutJob(req.Meta(), hash, js, nil)
            }
            fmt.Printf("Script out: %+v\n", out)
            WriteJSON(w, &out)
            return
        default:
            w.WriteHeader(http.StatusMethodNotAllowed)
            return
        }
    }
}
