/*

*/
package api

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/tsileo/blobstash/vkv"
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
func vkvHandler(db *vkv.DB, kvUpdate chan *vkv.KeyValue) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "GET":
			vars := mux.Vars(r)
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
			vars := mux.Vars(r)
			exists, err := db.Check(vars["key"])
			if err != nil {
				panic(err)
			}
			if exists {
				return
			}
			http.Error(w, http.StatusText(404), 404)
			return
		case "PUT":
			vars := mux.Vars(r)
			k := vars["key"]
			v := r.FormValue("value")
			res, err := db.Put(k, v, -1)
			kvUpdate <- res
			if err != nil {
				panic(err)
			}
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

func New(db *vkv.DB, kvUpdate chan *vkv.KeyValue) *mux.Router {
	//db, err := vkv.New("devdb")
	//if err != nil {
	//	panic(err)
	//}
	//defer db.Close()
	r := mux.NewRouter()
	r.HandleFunc("/api/v1/vkv/key/{key}", vkvHandler(db, kvUpdate))
	r.HandleFunc("/api/v1/vkv/key/{key}/versions", vkvVersionsHandler(db))
	//http.Handle("/", r)
	//http.ListenAndServe(":8050", nil)
	return r
}
