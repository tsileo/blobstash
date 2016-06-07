/*

Package bewit implement a subset of the Hawk[1] authentication scheme (Single URI Authorization).

Designed for providing short-term access to a protected resource.

This scheme doesn't provide any way to transmit the credentials (use TLS).

This implementation slightly differs with the original Hawk lib (which is the specification):

 - No host/path support (they're set to "", since it's hard to discover the host/port server-side due to proxying and the HTTP protocol.
 - No `ext` support (Oz related, an Hawk extension)

Links

  [1]: https://github.com/hueniverse/hawk

*/
package bewit

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

const (
	headerVersion    = "1" // Hawk protocol version
	authType         = "bewit"
	headerStart      = "hawk."
	method           = "GET"
	payloadSeparator = `\`
)

var (
	ErrEmptyBewit         = errors.New("Empty bewit")
	ErrInvalidMethod      = errors.New("Invalid method")
	ErrInvalidEncoding    = errors.New("Invalid bewit encoding")
	ErrInvalidPayload     = errors.New("Invalid bewit payload")
	ErrUnknownCredentials = errors.New("Unknown credentials")
	ErrInvalidTimestamp   = errors.New("Invalid timestamp")
	ErrAccessExpired      = errors.New("Access expired")
	ErrBadMac             = errors.New("Bad mac")
)

type Cred struct {
	ID  string
	Key []byte
}

func generateNormalizedString(expiration, method, resource string) []byte {
	var buf bytes.Buffer
	buf.WriteString(headerStart)
	buf.WriteString(headerVersion)
	buf.WriteString(".")
	buf.WriteString(authType)
	buf.WriteString(`\n`)
	buf.WriteString(expiration)
	buf.WriteString(`\n\n`) // Double new line, since no nonce neede for the bewit
	buf.WriteString(method)
	buf.WriteString(`\n`)
	buf.WriteString(resource)
	buf.WriteString(`\n\n\n`) // host, port, and hash empty
	return buf.Bytes()
}

func computeMac(creds *Cred, expiration, method, resource string) string {
	normalized := generateNormalizedString(expiration, method, resource)

	mac := hmac.New(sha256.New, creds.Key)
	mac.Write(normalized)
	return base64.StdEncoding.EncodeToString([]byte(mac.Sum(nil)))
}

func Bewit(creds *Cred, url *url.URL, ttl time.Duration) error {
	expiration := strconv.FormatInt(time.Now().Add(ttl).Unix(), 10)
	resource := buildResource(url)

	mac := computeMac(creds, expiration, "GET", resource)
	var bewit bytes.Buffer
	bewit.WriteString(creds.ID)
	bewit.WriteString(payloadSeparator)
	bewit.WriteString(expiration)
	bewit.WriteString(payloadSeparator)
	bewit.WriteString(mac)
	bewit.WriteString(payloadSeparator)
	// No ext support so we leave a trailing antislash

	q := url.Query()
	q.Add("bewit", base64.URLEncoding.EncodeToString(bewit.Bytes()))
	url.RawQuery = q.Encode()
	return nil
}

// Build the resource arg
func buildResource(url *url.URL) string {
	resource := url.Path
	if url.RawQuery != "" {
		resource += "?" + url.RawQuery
	}
	return resource
}

func Validate(req *http.Request, creds *Cred) error {
	now := time.Now()

	// Extract the bewit
	bewit := req.URL.Query().Get("bewit")
	if bewit == "" {
		return ErrEmptyBewit
	}
	q := req.URL.Query()
	q.Del("bewit")
	req.URL.RawQuery = q.Encode()

	// Check the method
	if req.Method != "GET" && req.Method != "HEAD" {
		return ErrInvalidMethod
	}

	// Decode the bewit
	rawBewit, err := base64.URLEncoding.DecodeString(bewit)
	if err != nil {
		return ErrInvalidEncoding
	}

	parts := bytes.SplitN(rawBewit, []byte(payloadSeparator), -1)
	if len(parts) < 3 {
		return ErrInvalidPayload
	}

	id := string(parts[0])
	if creds.ID != id {
		return ErrUnknownCredentials
	}

	bewitExp := string(parts[1])
	ts, err := strconv.ParseInt(bewitExp, 10, 64)
	if err != nil {

		return ErrInvalidTimestamp
	}
	bewitMac := parts[2]
	bewitTs := time.Unix(ts, 0)
	if now.After(bewitTs) {
		return ErrAccessExpired
	}

	resource := buildResource(req.URL)
	mac := []byte(computeMac(creds, bewitExp, method, resource))

	if !hmac.Equal(mac, bewitMac) {
		return ErrBadMac
	}

	// Authentication successful
	return nil
}
