package httputil

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"net/http"
	"strings"
)

// FIXME(tsileo): remove this package

// BasicRealm is used when setting the WWW-Authenticate response header.
var BasicRealm = "Authorization Required"

func BasicAuthFunc(username string, password string) func(*http.Request) bool {
	return func(req *http.Request) bool {
		// fmt.Printf("\n\nINSIDE BASICAUTH %+v\n\n", req)
		auth := req.Header.Get("Authorization")
		switch {
		case strings.HasPrefix(auth, "Basic "):
			siteAuth := base64.StdEncoding.EncodeToString([]byte(username + ":" + password))
			if secureCompare(auth, "Basic "+siteAuth) {
				return true
			}
			return false
		case req.URL.Query().Get("api_key") != "":
			fmt.Printf("\n\nAPI_KE %v %v\n\n", req.URL.Query().Get("api_key"), password)
			if secureCompare(req.URL.Query().Get("api_key"), password) {
				return true
			}
			return false
		case strings.HasPrefix(auth, "key "):
			if secureCompare(auth, "key "+password) {
				return true
			}
			return false
		}
		return false
	}
}

// secureCompare performs a constant time compare of two strings to limit timing attacks.
func secureCompare(given string, actual string) bool {
	givenSha := sha256.Sum256([]byte(given))
	actualSha := sha256.Sum256([]byte(actual))

	return subtle.ConstantTimeCompare(givenSha[:], actualSha[:]) == 1
}
