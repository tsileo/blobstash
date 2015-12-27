/*

Package bewit implement Hawk bewit authentication mechanism

See https://github.com/hueniverse/hawk

*/
package bewit

import (
	"crypto/sha256"
	"net/http"
	"time"

	"github.com/tent/hawk-go"
)

// FIXME(tsileo) generate/store a secure key
var key = "mysuperkey"
var appID = "luascripts"

// Since we only use hawk bewit auth, there's no nonce handling for us
func nonceCheckFunc(nonce string, t time.Time, cred *hawk.Credentials) bool {
	panic("should never be called")
	return true

}

func credentialsLookupFunc(cred *hawk.Credentials) error {
	cred.Hash = sha256.New
	cred.Key = key
	return nil
}

// New returns a `bewit` token valid for the given dealy
func New(url string, delay time.Duration) (string, error) {
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
func Check(r *http.Request) error {
	hawkAuth, err := hawk.NewAuthFromRequest(r, credentialsLookupFunc, nonceCheckFunc)
	if err != nil {
		return err
	}
	return hawkAuth.Valid()
}
