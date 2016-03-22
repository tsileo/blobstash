package httputil

import (
	"crypto/sha256"
	"net/http"
	"strings"
	"time"

	"github.com/tent/hawk-go"
)

/*

Implement Hawk bewit authentication mechanism

See https://github.com/hueniverse/hawk

*/

var key = ""
var appID = "blobstash"

func SetHawkKey(bkey []byte) {
	key = string(bkey)
}

func SetHawkAppID(newAppID string) {
	appID = newAppID
}

// Since we only use hawk bewit auth, there's no nonce handling for us
func nonceCheckFunc(nonce string, t time.Time, cred *hawk.Credentials) bool {
	panic("should never be called")
	return true
}

func credentialsLookupFunc(cred *hawk.Credentials) error {
	if key == "" {
		panic("Hawk key not set")
	}
	cred.Hash = sha256.New
	cred.Key = key
	return nil
}

// New returns a `bewit` token valid for the given dealy
func NewBewit(url string, delay time.Duration) (string, error) {
	if key == "" {
		panic("Hawk key not set")
	}
	// TODO(tsileo): submit an issue in the hawk lib, the way it cleans path
	// break URL without host, so I had to add **8** spaces to get it work. (hawk.goL300).
	// Also, when checking the bewit server-side, we can't know the host/scheme (proxy, GET /), so
	// we don't include it when generating the bewit.
	if strings.HasPrefix(url, "/") {
		url = "        " + url
	}
	auth, err := hawk.NewURLAuth(url, &hawk.Credentials{
		ID:   appID,
		Key:  key,
		Hash: sha256.New,
	}, delay)
	if err != nil {
		return "", err
	}
	return auth.Bewit(), nil
}

// Check will try to authenticate the `bewit` parameter for the given request
func CheckBewit(r *http.Request) error {
	hawkAuth, err := hawk.NewAuthFromRequest(r, credentialsLookupFunc, nonceCheckFunc)
	if err != nil {
		return err
	}
	return hawkAuth.Valid()
}
