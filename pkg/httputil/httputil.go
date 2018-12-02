package httputil // import "a4.io/blobstash/pkg/httputil"

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"

	"github.com/golang/snappy"
	"github.com/vmihailenco/msgpack"

	"a4.io/blobstash/pkg/logger"
)

const ResponseFormatHeader = "BlobStash-API-Response-Format"
const (
	jsonMimeType    = "application/json"
	msgpackMimeType = "application/msgpack"
)

func WithStatusCode(status int) func(http.ResponseWriter) {
	return func(w http.ResponseWriter) {
		w.WriteHeader(status)
	}
}

// FIXME(tsileo): a EncodeAndWrite  for []byte that support plain-text, snappy or lz4?

func Unmarshal(req *http.Request, out interface{}) error {
	requestFormat := jsonMimeType
	if f := req.Header.Get("Content-Type"); f != "" {
		requestFormat = f
	}

	switch requestFormat {
	case jsonMimeType:
		return json.NewDecoder(req.Body).Decode(out)
	case msgpackMimeType:
		return msgpack.NewDecoder(req.Body).Decode(out)
	}

	return fmt.Errorf("Unsupported request content type: \"%s\"", requestFormat)
}

func Read(r *http.Request) ([]byte, error) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}

	// FIXME(tsileo): use a sync.Pool for the snappy reader (thanks to Reset on the snappy reader)
	if r.Header.Get("Content-Type") == "snappy" {
		return snappy.Decode(nil, body)
	}

	return body, nil
}

// Same as Write, but assume data is already snappy encoded
func WriteEncoded(r *http.Request, w http.ResponseWriter, data []byte, writeOptions ...func(http.ResponseWriter)) {
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Content-Encoding", "snappy")

	for _, wo := range writeOptions {
		wo(w)
	}

	if _, err := w.Write(data); err != nil {
		panic(err)
	}
}

func Write(r *http.Request, w http.ResponseWriter, data []byte, writeOptions ...func(http.ResponseWriter)) {
	w.Header().Set("Cache-Control", "no-cache")

	var snap bool
	if e := r.Header.Get("Accept-Encoding"); e == "snappy" {
		w.Header().Set("Content-Encoding", e)
		snap = true
	}

	for _, wo := range writeOptions {
		wo(w)
	}

	if snap {
		if _, err := w.Write(snappy.Encode(nil, data)); err != nil {
			panic(err)
		}
		return
	}

	if _, err := w.Write(data); err != nil {
		panic(err)
	}
}

func MarshalAndWrite(r *http.Request, w http.ResponseWriter, data interface{}, writeOptions ...func(http.ResponseWriter)) bool {
	responseFormat := jsonMimeType
	if f := r.Header.Get("Accept"); f != "" && f != "*/*" {
		responseFormat = strings.TrimSpace(strings.Split(f, ",")[0])
	}

	w.Header().Set("Content-Type", responseFormat)

	for _, wo := range writeOptions {
		wo(w)
	}

	var out []byte
	var err error

	switch responseFormat {
	case jsonMimeType:
		out, err = json.Marshal(data)
	case msgpackMimeType:
		out, err = msgpack.Marshal(data)
	default:
		// Return a 406
		msg := fmt.Sprintf("Requested encoding \"%s\" (via Accept) is not supported, try: application/json", responseFormat)
		http.Error(w, msg, http.StatusNotAcceptable) // 406
		return false
	}

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return false
	}

	// Write the response (and optionally compress the response with snappy)
	Write(r, w, out)
	return true
}

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

// GetSessionID returns the client "session ID" (set via the `BlobStash-Session-ID` header).
// Returns an empty string if it's missing.
func GetSessionID(r *http.Request) string {
	return r.Header.Get("BlobStash-Session-ID")
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

// SetAttachment will set the "Content-Disposition" header if the "dl" query parameter is set
func SetAttachment(fname string, r *http.Request, w http.ResponseWriter) {
	// Check if the file is requested for download
	if r.URL.Query().Get("dl") != "" {
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s", fname))
	}
}

type Query struct {
	values url.Values
}

func NewQuery(q url.Values) *Query {
	return &Query{q}
}

func (q *Query) Get(key string) string {
	return q.values.Get(key)
}

func (q *Query) GetDefault(key, defaultval string) string {
	if v := q.values.Get(key); v != "" {
		return v
	}
	return defaultval
}

func (q *Query) GetBoolDefault(key string, defaultval bool) (bool, error) {
	if sv := q.values.Get(key); sv != "" {
		val, err := strconv.ParseBool(sv)
		if err != nil {
			return false, fmt.Errorf("failed to parse %s as bool: %v", key, err)
		}

		return val, nil
	}

	// Return the default value
	return defaultval, nil
}

func (q *Query) GetInt64Default(key string, defaultval int64) (int64, error) {
	if sv := q.values.Get(key); sv != "" {
		val, err := strconv.ParseInt(sv, 10, 0)
		if err != nil {
			return 0, fmt.Errorf("failed to parse %s as int: %v", key, err)
		}

		return val, nil
	}

	// Return the default value
	return defaultval, nil
}

func (q *Query) GetIntDefault(key string, defaultval int) (int, error) {
	if sv := q.values.Get(key); sv != "" {
		val, err := strconv.Atoi(sv)
		if err != nil {
			return 0, fmt.Errorf("failed to parse %s as int: %v", key, err)
		}

		return val, nil
	}

	// Return the default value
	return defaultval, nil
}

func (q *Query) GetInt(key string, defaultval, maxval int) (int, error) {
	if sv := q.values.Get(key); sv != "" {
		val, err := strconv.Atoi(sv)
		if err != nil {
			return 0, fmt.Errorf("failed to parse %s: %v", key, err)
		}

		// Check the boundaries
		if val > maxval {
			val = maxval
		}

		return val, nil
	}

	// Return the default value
	return defaultval, nil
}
