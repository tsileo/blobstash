/*

*/
package main

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
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

type VkvItem struct {
	Key     string `json:"key,omitempty"`
	Value   string `json:"value"`
	Version int    `json:"version"`
}

func vkvHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "GET":
			vars := mux.Vars(r)
			res := &VkvItem{
				Key: vars["key"],
			}
			WriteJSON(w, res)
		case "HEAD":
			exists := true
			if exists {
				return
			}
			http.Error(w, http.StatusText(404), 404)
			return
		case "PUT":
			vars := mux.Vars(r)
			k := vars["key"]
			v := r.FormValue("value")
			res := &VkvItem{
				Key:   k,
				Value: v,
			}
			WriteJSON(w, res)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}
}

type VersionsReponse struct {
	Key      string     `json:"key"`
	Versions []*VkvItem `json:"versions"`
}

func vkvVersionsHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "GET":
			vars := mux.Vars(r)
			res := &VersionsReponse{
				Key:      vars["key"],
				Versions: []*VkvItem{},
			}
			WriteJSON(w, res)
			return
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}
}

func main() {
	r := mux.NewRouter()
	r.HandleFunc("/api/v1/vkv/{key}", vkvHandler())
	r.HandleFunc("/api/v1/vkv/{key}/versions", vkvVersionsHandler())
	http.Handle("/", r)
	http.ListenAndServe(":8050", nil)
}
