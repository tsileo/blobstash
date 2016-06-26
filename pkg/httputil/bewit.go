package httputil

import (
	"net/http"
	"net/url"
	"time"

	"github.com/tsileo/blobstash/pkg/httputil/bewit"
)

/*

Implement Hawk bewit authentication mechanism

See https://github.com/hueniverse/hawk

*/

var creds = &bewit.Cred{}

func SetHawkKey(bkey []byte) {
	creds.Key = bkey
	creds.ID = "blobstash"
}

// New returns a `bewit` token valid for the given TTL
func NewBewit(url *url.URL, ttl time.Duration) error {
	// FIXME(tsileo): take a `*url.URL` as argument
	if len(creds.Key) == 0 {
		panic("Hawk key not set")
	}
	if err := bewit.Bewit(creds, url, ttl); err != nil {
		return err
	}
	return nil
}

// Check will try to authenticate the `bewit` parameter for the given request
func CheckBewit(r *http.Request) error {
	return bewit.Validate(r, creds)
}
