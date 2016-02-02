package httputil

import (
	"net/http"

	log "gopkg.in/inconshreveable/log15.v2"
)

func LoggerMiddleware(logger log.Logger) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			next.ServeHTTP(w, r)
			log.Info(r.URL.String(), "method", r.Method, "len", r.ContentLength, "proto", r.Proto, "ip", GetIpAddress(r))
		})
	}
}
