package js // import "a4.io/blobstash/pkg/js"

import (
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
)

func Get(name string) string {
	dat, ok := files[name]
	if !ok {
		panic(fmt.Sprintf("missing file %s", name))
	}
	return dat
}

func Register(r *mux.Router, basicAuth func(http.Handler) http.Handler) {
	for k, content := range files {
		r.Handle(fmt.Sprintf("/%s", k), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/javascript")
			w.Write([]byte(content))
		}))
	}
}
