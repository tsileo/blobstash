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
	"fmt"
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

type Creds struct {
	ID  string
	Key []byte
}

func generateNormalizedString(expiration, method, resource string) []byte {
	var buf bytes.Buffer
	buf.WriteString(headerStart)
	buf.WriteString(headerVersion)
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

func computeMac(creds *Creds, expiration, method, resource string) string {
	normalized := generateNormalizedString(expiration, method, resource)

	mac := hmac.New(sha256.New, creds.Key)
	mac.Write(normalized)
	return base64.StdEncoding.EncodeToString([]byte(mac.Sum(nil)))
}

func Bewit(creds *Creds, url *url.URL, ttl time.Duration) (string, error) {
	expiration := strconv.FormatInt(time.Now().Add(ttl).Unix(), 10)
	mac := computeMac(creds, expiration, "GET", url.Path)
	var bewit bytes.Buffer
	bewit.WriteString(creds.ID)
	bewit.WriteString(payloadSeparator)
	bewit.WriteString(expiration)
	bewit.WriteString(payloadSeparator)
	bewit.WriteString(mac)
	bewit.WriteString(payloadSeparator)
	// No ext support so we leave a trailing antislash

	fmt.Printf("bewit=%s", bewit.String())
	return base64.URLEncoding.EncodeToString(bewit.Bytes()), nil
}

func Validate(req *http.Request, creds *Creds) error {
	now := time.Now()

	// Extract the bewit
	bewit := req.URL.Query().Get("bewit")
	if bewit == "" {
		return errors.New("Empty bewit")
	}
	req.URL.Query().Del("bewit")

	// Check the method
	if req.Method != "GET" && req.Method != "HEAD" {
		return errors.New("Invalid method")
	}

	// Build the resource arg
	resource := req.URL.Path
	if req.URL.RawQuery != "" {
		resource += "?" + req.URL.RawQuery
	}

	// Decode the bewit
	rawBewit, err := base64.URLEncoding.DecodeString(bewit)
	if err != nil {
		return errors.New("Invalid bewit encoding")
	}

	parts := bytes.SplitN(rawBewit, []byte(payloadSeparator), -1)
	if len(parts) < 3 {
		return errors.New("Invalid bewit payload")
	}

	id := string(parts[0])
	if creds.ID != id {
		return errors.New("Unknown credentials")
	}

	bewitExp := string(parts[1])
	ts, err := strconv.ParseInt(bewitExp, 10, 64)
	if err != nil {
		return errors.New("Invalid timestamp")
	}
	bewitMac := parts[2]
	if time.Unix(ts, 0).After(now) {
		return errors.New("Access expired")
	}

	mac := []byte(computeMac(creds, bewitExp, method, resource))

	if !hmac.Equal(mac, bewitMac) {
		return errors.New("Bad mac")
	}
	return nil
}
