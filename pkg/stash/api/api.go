package api // import "a4.io/blobstash/pkg/stash/api"

import (
	"context"
	"fmt"
	"net/http"

	humanize "github.com/dustin/go-humanize"
	"github.com/gorilla/mux"

	"a4.io/blobstash/pkg/ctxutil"
	"a4.io/blobstash/pkg/httputil"
	"a4.io/blobstash/pkg/hub"
	"a4.io/blobstash/pkg/stash"
	"a4.io/blobstash/pkg/stash/gc"
	"a4.io/blobstash/pkg/stash/store"
)

type StashAPI struct {
	stash *stash.Stash
	hub   *hub.Hub
}

func New(s *stash.Stash, h *hub.Hub) *StashAPI {
	return &StashAPI{s, h}
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

type GCInput struct {
	Script string `json:"script" msgpack:"script"`
}

func (s *StashAPI) dataContextGCHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		name := mux.Vars(r)["name"]
		ctx = ctxutil.WithNamespace(ctx, name)

		_, ok := s.stash.DataContextByName(name)
		switch r.Method {
		case "POST":
			defer r.Body.Close()
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			out := &GCInput{}
			if err := httputil.Unmarshal(r, out); err != nil {
				panic(err)
			}
			fmt.Printf("\n\nGC imput: %+v\n\n", out)
			if err := s.stash.DoAndDestroy(ctx, name, func(ctx context.Context, dc store.DataContext) error {
				blobs, size, err := gc.GC(ctx, s.hub, s.stash, dc, out.Script, map[string]struct{}{})
				fmt.Printf("GC err=%v, output: %d blobs,  %s\n\n", err, blobs, humanize.Bytes(size))
				return err

			}); err != nil {
				panic(err)
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}
}

type GC2Input struct {
	Ref     string `json:"ref" msgpack:"ref"`
	Version int64  `json:"version" msgpack:"version"`
}

func (s *StashAPI) dataContextGC2Handler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		name := mux.Vars(r)["name"]
		ctx = ctxutil.WithNamespace(ctx, name)

		_, ok := s.stash.DataContextByName(name)
		switch r.Method {
		case "POST":
			defer r.Body.Close()
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			out := &GC2Input{}
			if err := httputil.Unmarshal(r, out); err != nil {
				panic(err)
			}
			fmt.Printf("\n\nGC imput: %+v\n\n", out)
			if err := s.stash.MergeFileTreeVersionAndDestroy(ctx, name, out.Ref, out.Version); err != nil {
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
	r.Handle("/{name}/_merge_filetree_version", basicAuth(http.HandlerFunc(s.dataContextGC2Handler())))
}
