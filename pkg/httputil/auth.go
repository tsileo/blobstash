package httputil

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"net/http"
)

// FIXME(tsileo): remove this package

// BasicRealm is used when setting the WWW-Authenticate response header.
var BasicRealm = "Authorization Required"

func BasicAuthFunc(username string, password string) func(*http.Request) bool {
	return func(req *http.Request) bool {
		var siteAuth = base64.StdEncoding.EncodeToString([]byte(username + ":" + password))
		auth := req.Header.Get("Authorization")
		if !secureCompare(auth, "Basic "+siteAuth) {
			return false
		}
		return true
	}
}

// secureCompare performs a constant time compare of two strings to limit timing attacks.
func secureCompare(given string, actual string) bool {
	givenSha := sha256.Sum256([]byte(given))
	actualSha := sha256.Sum256([]byte(actual))

	return subtle.ConstantTimeCompare(givenSha[:], actualSha[:]) == 1
}
