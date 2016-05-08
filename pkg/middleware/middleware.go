package middleware

import (
	"net/http"
	"os"
	"strconv"

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
		FrameDeny:             true,
		ContentTypeNosniff:    true,
		BrowserXssFilter:      true,
		ContentSecurityPolicy: "default-src 'self'",
		IsDevelopment:         isDevelopment,
	}
	// var tlsHostname string
	// if tlsHost, ok := s.conf["tls-hostname"]; ok {
	// 	tlsHostname = tlsHost.(string)
	// 	secureOptions.AllowedHosts = []string{tlsHostname}
	// }
	return secure.New(secureOptions).Handler(h)
}

func BasicAuth(h http.Handler) http.Handler {
	return nil
	// authMiddleware := middleware.BasicAuthFunc(s.perms.AuthFunc)
}
