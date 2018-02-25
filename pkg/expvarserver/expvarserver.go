package expvarserver

import (
	_ "expvar"
	"net/http"

	"a4.io/blobstash/pkg/config"
)

func Enable(conf *config.Config) error {
	return http.ListenAndServe(conf.ExpvarListen, http.DefaultServeMux)
}
