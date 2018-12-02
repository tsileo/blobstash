package httputil

import (
	"crypto/rand"
	"encoding/hex"
	"expvar"
	"net/http"
	"time"

	log "github.com/inconshreveable/log15"
)

var (
	apiReqsVar = expvar.NewInt("api-reqs")
)

func newReqID() string {
	bytes := make([]byte, 4)
	if _, err := rand.Read(bytes); err != nil {
		panic(err)
	}
	return hex.EncodeToString(bytes)
}

func ExpvarsMiddleware(m *expvar.Map) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			apiReqsVar.Add(1)
			next.ServeHTTP(w, r)
		})
	}
}

// tiny http.ResponseWriter for deferring the WriteHeader call once the debug headers has been added
type crw struct {
	http.ResponseWriter
	statusCode int
	written    bool
}

// Write overrides the default Write to write and track the response status code
func (rw *crw) Write(data []byte) (int, error) {
	rw.written = true
	rw.WriteHeader(rw.statusCode)
	return rw.ResponseWriter.Write(data)
}

// WriteHeader overrides the default WriteHeader, it will be set once all the debug headers has been added
func (rw *crw) WriteHeader(status int) {
	rw.statusCode = status
}

// HeaderLog append a debug message that will be outputted in the `BlobStash-Debug` header
func HeaderLog(w http.ResponseWriter, msg string) {
	w.Header().Add("BlobStash-Debug", msg)
}

// LoggerMiddleware logs HTTP requests and adds some debug headers
func LoggerMiddleware(logger log.Logger) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			reqID := newReqID()
			start := time.Now()
			rw := &crw{w, 200, false}
			next.ServeHTTP(rw, r)
			resp_time := time.Since(start)
			w.Header().Set("BlobStash-Resp-Time", resp_time.String())
			w.Header().Set("BlobStash-Req-ID", reqID)
			if !rw.written {
				w.WriteHeader(rw.statusCode)
			}
			log.Info(r.URL.String(), "method", r.Method, "status_code", rw.statusCode, "len", r.ContentLength, "proto", r.Proto,
				"resp_time", time.Since(start), "ip", GetIpAddress(r), "req_id", reqID)
		})
	}
}
