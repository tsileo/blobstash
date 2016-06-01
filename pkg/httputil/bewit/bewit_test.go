package bewit

import (
	_ "fmt"
	"net/http"
	"net/url"
	"testing"
	"time"
)

func check(err error) {
	if err != nil {
		panic(err)
	}
}

var (
	creds1    = &Creds{ID: "id1", Key: []byte("key1")}
	resource1 = "/resource1"
	resource2 = "/resource2"
)

var testTable = []struct {
	creds          *Creds
	url            *url.URL
	exp            time.Duration
	alterURLFunc   func(*url.URL) // Alter the URL after the bewit generation
	alterCredsFunc func(*Creds)
	method         string
	expected       error // Expected `Validate` result
}{
	{
		creds:          creds1,
		url:            &url.URL{Path: resource1},
		exp:            1 * time.Minute,
		alterURLFunc:   nil,
		alterCredsFunc: nil,
		method:         "GET",
		expected:       nil,
	},
	{
		creds:          creds1,
		url:            &url.URL{Path: resource1, RawQuery: "k1=v1&k2=v2"},
		exp:            1 * time.Minute,
		alterURLFunc:   nil,
		alterCredsFunc: nil,
		method:         "GET",
		expected:       nil,
	},
	{
		creds:          creds1,
		url:            &url.URL{Path: resource1},
		exp:            1 * time.Minute,
		alterURLFunc:   nil,
		alterCredsFunc: nil,
		method:         "HEAD",
		expected:       nil,
	},
	{
		creds: creds1,
		url:   &url.URL{Path: resource1},
		exp:   1 * time.Minute,
		alterURLFunc: func(u *url.URL) {
			// Remove the bewit query arg
			q := u.Query()
			q.Del("bewit")
			u.RawQuery = q.Encode()
		},
		alterCredsFunc: nil,
		method:         "GET",
		expected:       ErrEmptyBewit,
	},
	{
		creds:          creds1,
		url:            &url.URL{Path: resource1},
		exp:            1 * time.Minute,
		alterURLFunc:   nil,
		alterCredsFunc: nil,
		method:         "POST",
		expected:       ErrInvalidMethod,
	},
	{
		creds: creds1,
		url:   &url.URL{Path: resource1},
		exp:   1 * time.Minute,
		alterURLFunc: func(u *url.URL) {
			q := u.Query()
			q.Set("bewit", "izznvalidencoding")
			u.RawQuery = q.Encode()
		},
		alterCredsFunc: nil,
		method:         "GET",
		expected:       ErrInvalidEncoding,
	},
	{
		creds: creds1,
		url:   &url.URL{Path: resource1},
		exp:   1 * time.Minute,
		alterURLFunc: func(u *url.URL) {
			q := u.Query()
			q.Set("bewit", "invalidencoding=")
			u.RawQuery = q.Encode()
		},
		alterCredsFunc: nil,
		method:         "GET",
		expected:       ErrInvalidPayload,
	},
	{
		creds:        creds1,
		url:          &url.URL{Path: resource1},
		exp:          1 * time.Minute,
		alterURLFunc: nil,
		alterCredsFunc: func(c *Creds) {
			c.ID = "t"
		},
		method:   "GET",
		expected: ErrUnknownCredentials,
	},
	// TODO(tsileo): test invalid timestamp
	{
		creds:          creds1,
		url:            &url.URL{Path: resource1},
		exp:            -5 * time.Minute,
		alterURLFunc:   nil,
		alterCredsFunc: nil,
		method:         "GET",
		expected:       ErrAccessExpired,
	},
	{
		creds: creds1,
		url:   &url.URL{Path: resource1},
		exp:   1 * time.Minute,
		alterURLFunc: func(u *url.URL) {
			// Change the path to trigger an `ErrBadMac` error
			u.Path = resource2
		},
		alterCredsFunc: nil,
		method:         "GET",
		expected:       ErrBadMac,
	},
	{
		creds: creds1,
		url:   &url.URL{Path: resource2},
		exp:   1 * time.Minute,
		alterURLFunc: func(u *url.URL) {
			// Change the query arg to trigger an `ErrBadMac` error
			q := u.Query()
			q.Add("new", "arg")
			u.RawQuery = q.Encode()
		},
		alterCredsFunc: nil,
		method:         "GET",
		expected:       ErrBadMac,
	},
}

func TestBewit(t *testing.T) {
	for _, tdata := range testTable {
		check(Bewit(tdata.creds, tdata.url, tdata.exp))

		if tdata.alterURLFunc != nil {
			tdata.alterURLFunc(tdata.url)
		}
		if tdata.alterCredsFunc != nil {
			tdata.alterCredsFunc(tdata.creds)
		}

		req := &http.Request{
			URL:    tdata.url,
			Method: tdata.method,
		}

		if err := Validate(req, tdata.creds); err != tdata.expected {
			t.Errorf("Failed to validate bewit, got: %v, expected: %v", err, tdata.expected)
		}
	}
}
