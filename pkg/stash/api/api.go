package api // import "a4.io/blobstash/pkg/stash/api"

import (
	"context"
	"net/http"

	"github.com/gorilla/mux"

	"a4.io/blobstash/pkg/ctxutil"
	"a4.io/blobstash/pkg/httputil"
	"a4.io/blobstash/pkg/stash"
	"a4.io/blobstash/pkg/stash/gc"
	"a4.io/blobstash/pkg/stash/store"
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
		httputil.MarshalAndWrite(r, w, map[string]interface{}{
			"data": s.stash.ContextNames(),
		})
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
			if r.Method == "HEAD" {
				return
			}
			httputil.MarshalAndWrite(r, w, map[string]interface{}{
				"data": nil,
			})
		case "DELETE":
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			// FIXME(tsileo): stash.Destroy should destroy the data ctx
			dataContext.Destroy()
			if err := s.stash.Destroy(context.TODO(), name); err != nil {
				panic(err)
			}
			w.WriteHeader(http.StatusNoContent)

		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}
}

func (s *StashAPI) dataContextMergeHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		name := mux.Vars(r)["name"]
		_, ok := s.stash.DataContextByName(name)
		switch r.Method {
		case "POST":
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			if err := s.stash.MergeAndDestroy(context.TODO(), name); err != nil {
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
		ctx := r.Context()
		name := mux.Vars(r)["name"]
		ctx = ctxutil.WithNamespace(ctx, name)

		_, ok := s.stash.DataContextByName(name)
		switch r.Method {
		case "POST":
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			script, err := httputil.Read(r)
			if err != nil {
				panic(err)
			}
			defer r.Body.Close()
			if err := s.stash.DoAndDestroy(ctx, name, func(ctx context.Context, dc store.DataContext) error {
				return gc.GC(ctx, nil, s.stash, string(script), nil)

			}); err != nil {
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
