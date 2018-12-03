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

func newCustomResponseWriter(w http.ResponseWriter) *crw {
	return &crw{
		reqID:          newReqID(),
		ResponseWriter: w,
		statusCode:     200,
		written:        false,
		start:          time.Now(),
	}
}

// tiny http.ResponseWriter for deferring the WriteHeader call once the debug headers has been added
type crw struct {
	http.ResponseWriter
	reqID      string
	statusCode int
	written    bool
	start      time.Time
}

func (rw *crw) writeHeaderIfNeeded() {
	if !rw.written {
		rw.written = true
		rw.ResponseWriter.WriteHeader(rw.statusCode)
	}
}

// Write overrides the default Write to write and track the response status code
func (rw *crw) Write(data []byte) (int, error) {
	rw.writeHeaderIfNeeded()
	return rw.ResponseWriter.Write(data)
}

// WriteHeader overrides the default WriteHeader, it will be set once all the debug headers has been added
func (rw *crw) WriteHeader(status int) {
	rw.statusCode = status
}

func (rw *crw) ReqID() string {
	return rw.reqID
}

func (rw *crw) RespTime() time.Duration {
	return time.Since(rw.start)
}

// HeaderLog append a debug message that will be outputted in the `BlobStash-Debug` header
func HeaderLog(w http.ResponseWriter, msg string) {
	w.Header().Add("Blobstash-Debug", msg)
}

// LoggerMiddleware logs HTTP requests and adds some debug headers
func LoggerMiddleware(logger log.Logger) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			rw := newCustomResponseWriter(w)

			next.ServeHTTP(rw, r)

			resp_time := rw.RespTime()
			w.Header().Set("Blobstash-Resp-Time", resp_time.String())
			w.Header().Set("Blobstash-Req-ID", rw.reqID)
			// Write the status code if needed
			rw.writeHeaderIfNeeded()
			log.Info(r.URL.String(), "method", r.Method, "status_code", rw.statusCode, "len", r.ContentLength, "proto", r.Proto,
				"resp_time", resp_time, "ip", GetIpAddress(r), "req_id", rw.reqID)
		})
	}
}
