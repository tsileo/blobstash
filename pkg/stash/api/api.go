package api // import "a4.io/blobstash/pkg/stash/api"

import (
	"context"
	"github.com/gorilla/mux"
	"io/ioutil"
	"net/http"

	"a4.io/blobstash/pkg/httputil"
	"a4.io/blobstash/pkg/stash"
	"a4.io/blobstash/pkg/stash/gc"
)

type StashAPI struct {
	stash *stash.Stash
}

func New(s *stash.Stash) *StashAPI {
	return &StashAPI{s}
}

func (s *StashAPI) listHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		srw := httputil.NewSnappyResponseWriter(w, r)
		httputil.WriteJSON(srw, map[string]interface{}{
			"data": s.stash.ContextNames(),
		})
		srw.Close()
	}
}

func (s *StashAPI) dataContextHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		name := mux.Vars(r)["name"]
		dataContext, ok := s.stash.DataContextByName(name)
		switch r.Method {
		case "GET", "HEAD":
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.WriteHeader(http.StatusOK)
			if r.Method == "HEAD" {
				return
			}
			srw := httputil.NewSnappyResponseWriter(w, r)
			httputil.WriteJSON(srw, map[string]interface{}{
				"data": nil,
			})
			srw.Close()
		case "DELETE":
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			dataContext.Destroy()
			w.WriteHeader(http.StatusNoContent)

		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}
}

func (s *StashAPI) dataContextMergeHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		name := mux.Vars(r)["name"]
		dataContext, ok := s.stash.DataContextByName(name)
		switch r.Method {
		case "POST":
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			if err := dataContext.Merge(context.TODO()); err != nil {
				panic(err)
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}
}

func (s *StashAPI) dataContextGCHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		name := mux.Vars(r)["name"]
		dataContext, ok := s.stash.DataContextByName(name)
		switch r.Method {
		case "POST":
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			script, err := ioutil.ReadAll(r.Body)
			if err != nil {
				panic(err)
			}
			defer r.Body.Close()
			garbageCollector := gc.New(s.stash, dataContext)
			if err := garbageCollector.GC(context.TODO(), string(script)); err != nil {
				panic(err)
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}
}

func (s *StashAPI) Register(r *mux.Router, basicAuth func(http.Handler) http.Handler) {
	r.Handle("/", basicAuth(http.HandlerFunc(s.listHandler())))
	r.Handle("/{name}", basicAuth(http.HandlerFunc(s.dataContextHandler())))
	r.Handle("/{name}/_merge", basicAuth(http.HandlerFunc(s.dataContextMergeHandler())))
	r.Handle("/{name}/_gc", basicAuth(http.HandlerFunc(s.dataContextGCHandler())))
}
