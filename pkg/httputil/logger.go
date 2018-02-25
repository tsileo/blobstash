package httputil

import (
	"expvar"
	"net/http"
	"time"

	log "github.com/inconshreveable/log15"
)

func ExpvarsMiddleware(m *expvar.Map) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			m.Add("reqs", 1)
			next.ServeHTTP(w, r)
		})
	}
}

func LoggerMiddleware(logger log.Logger) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			next.ServeHTTP(w, r)
			log.Info(r.URL.String(), "method", r.Method, "len", r.ContentLength, "proto", r.Proto,
				"resp_time", time.Since(start), "ip", GetIpAddress(r))
		})
	}
}
