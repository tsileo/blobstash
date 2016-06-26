package httputil

import (
	"github.com/tsileo/blobstash/pkg/logger"
	// "github.com/tsileo/blobstash/permissions"

	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strings"
)

// WriteJSON marshal and output the data as JSON with the right content-type
func WriteJSON(w http.ResponseWriter, data interface{}) {
	js, err := json.Marshal(data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(js)
}

// WriteJSONError is an helper to output a {"error": <msg>} JSON payload with the given status code
func WriteJSONError(w http.ResponseWriter, status int, msg string) {
	js, err := json.Marshal(map[string]interface{}{
		"error": msg,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	w.Write(js)
}

// Error is an shortcut for `WriteJSONError(w, http.StatusInternalServerError, err.Error())`
func Error(w http.ResponseWriter, err error) {
	WriteJSONError(w, http.StatusInternalServerError, err.Error())
}

// Set the `Cache-control` header to `no-cache` in order to prevent the browser to cache the response
func SetNoCache(w http.ResponseWriter) {
	w.Header().Set("Cache-control", "no-cache")
}

// Request.RemoteAddress contains port, which we want to remove i.e.:
// "[::1]:58292" => "[::1]"
func ipAddrFromRemoteAddr(s string) string {
	idx := strings.LastIndex(s, ":")
	if idx == -1 {
		return s
	}
	return s[:idx]
}

// Return the IP Address from the `*http.Request`.
// Try the `X-Real-Ip`, `X-Forwarded-For` headers first.
func GetIpAddress(r *http.Request) string {
	hdr := r.Header
	hdrRealIp := hdr.Get("X-Real-Ip")
	hdrForwardedFor := hdr.Get("X-Forwarded-For")
	if hdrRealIp == "" && hdrForwardedFor == "" {
		return ipAddrFromRemoteAddr(r.RemoteAddr)
	}
	if hdrForwardedFor != "" {
		// X-Forwarded-For is potentially a list of addresses separated with ","
		parts := strings.Split(hdrForwardedFor, ",")
		for i, p := range parts {
			parts[i] = strings.TrimSpace(p)
		}
		// TODO: should return first non-local address
		return parts[0]
	}
	return hdrRealIp
}

// Wrapping an error in PublicError will make the RecoverHandler display the error message
// instead of the default status text.
type PublicError struct {
	Err error
}

// Error implements the Error interface
func (pe *PublicError) Error() string {
	return pe.Err.Error()
}

// Status implements the PublicErrorer interface with a 500 status code
func (pe *PublicError) Status() int {
	return http.StatusInternalServerError
}

// NewPublicError is a shortcut for initializing a `PublicError` with `fmt.Errorf`
func NewPublicErrorFmt(msg string, args ...interface{}) PublicErrorer {
	return &PublicError{fmt.Errorf(msg, args...)}
}

// PublicErrorer is the interface for "displayable" error by the RecoverHandler
type PublicErrorer interface {
	Status() int
	Error() string
}

// RecoverHandler catches all the "paniced" errors and display a JSON error
func RecoverHandler(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			// FIXME(tsileo): debug config should raise exception
			// r := recover()
			var r interface{}
			if r != nil {
				logger.Log.Error("request failed", "err", r, "type", reflect.TypeOf(r))
				switch t := r.(type) {
				default:
					if pe, ok := t.(PublicErrorer); ok {
						WriteJSONError(w, pe.Status(), pe.Error())
						return
					}
				}
				WriteJSONError(w, http.StatusInternalServerError, http.StatusText(http.StatusInternalServerError))
				return
			}
		}()
		h.ServeHTTP(w, r)
	})
}
