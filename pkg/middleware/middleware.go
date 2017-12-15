package middleware // import "a4.io/blobstash/pkg/middleware"

import (
	"net/http"
	"os"
	"strconv"

	"a4.io/blobstash/pkg/config"
	"a4.io/blobstash/pkg/httputil"

	_ "github.com/carbocation/interpose/middleware"
	"github.com/unrolled/secure"
)

func Secure(h http.Handler) http.Handler {
	// FIXME allowedorigins from config
	isDevelopment, _ := strconv.ParseBool(os.Getenv("BLOBSTASH_DEV_MODE"))
	// if isDevelopment {
	// 	s.Log.Info("Server started in development mode")
	// }
	secureOptions := secure.Options{
		FrameDeny:          true,
		ContentTypeNosniff: true,
		BrowserXssFilter:   true,
		IsDevelopment:      isDevelopment,
	}
	// var tlsHostname string
	// if tlsHost, ok := s.conf["tls-hostname"]; ok {
	// 	tlsHostname = tlsHost.(string)
	// 	secureOptions.AllowedHosts = []string{tlsHostname}
	// }
	return secure.New(secureOptions).Handler(h)
}

func CorsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Headers", "Authorization, Accept")
		w.Header().Set("Access-Control-Allow-Methods", "POST, PATCH, GET, OPTIONS, DELETE, PUT")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		if r.Method == "OPTIONS" {
			w.WriteHeader(200)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func NewBasicAuth(conf *config.Config) (func(*http.Request) bool, func(http.Handler) http.Handler) {
	// FIXME(tsileo): clean this, and load passfrom config
	if conf.APIKey == "" {
		return nil, func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				next.ServeHTTP(w, r)
				return
			})
		}

	}
	authFunc := httputil.BasicAuthFunc("", conf.APIKey)
	return authFunc, func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if authFunc(r) {
				next.ServeHTTP(w, r)
				return
			}
			httputil.WriteJSONError(w, http.StatusUnauthorized, http.StatusText(http.StatusUnauthorized))
		})
	}
}
