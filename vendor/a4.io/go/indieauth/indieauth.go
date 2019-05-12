/*
Package indieauth implements an IndieAuth (an identity layer on top of OAuth 2.0)] client/authentication middleware.
*/
package indieauth // import "a4.io/go/indieauth"

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/gorilla/sessions"
	"github.com/hashicorp/golang-lru"
	"github.com/peterhellberg/link"
	"willnorris.com/go/microformats"
)

const (
	// DefaultRedirectPath is default path where the RedirectHandler should be served
	DefaultRedirectPath = "/indieauth-redirect"

	authEndpointRel = "authorization_endpoint"
)

var (
	// ErrForbidden is returned when the authorization endpoint answered a 403
	ErrForbidden = errors.New("authorization endpoint answered with forbidden")

	// ErrAuthorizationEndointNotFound is returned when the authorization_endpoint could not be discovered for the given URL
	ErrAuthorizationEndpointNotFound = errors.New("authorization_endpoint not found")

	// UserAgent is the User Agent used for the requests performed as an "IndieAuth Client"
	UserAgent = "IndieAuth client (+https://a4.io/go/indieauth)"

	// SessionName is the name of the Gorilla session
	SessionName = "indieauth"
)

// defaultClientID guess the client ID from the current request
func defaultClientID(r *http.Request) string {
	s := "https"
	if r.TLS == nil {
		s = "http"
	}
	return s + "://" + r.Host
}

// ClientID can optionally be used to force a specific client ID.
// IndieAuth.ClientID = indieauth.ClientID("https://my.client.tld")
func ClientID(clientID string) func(*http.Request) string {
	return func(_ *http.Request) string {
		return clientID
	}
}

// IndieAuth holds the auth manager
type IndieAuth struct {
	me           string
	authEndpoint string
	store        *sessions.CookieStore
	cache        *lru.Cache

	// ClientID will try to guess the client ID from the request by default
	ClientID func(r *http.Request) string
	// RedirectPath will default to `/indieauth-redirect`
	RedirectPath string
}

// New initializes an IndieAuth auth manager, the `Middleware` shortcut is the preferred API unless you want fine-grained configuration.
func New(store *sessions.CookieStore, me string) (*IndieAuth, error) {
	c, err := lru.New(64)
	if err != nil {
		return nil, err
	}
	authEndpoint, err := getAuthEndpoint(me)
	if err != nil {
		return nil, fmt.Errorf("failed to get \"authorization_endpoint\": %v", err)
	}
	ia := &IndieAuth{
		me:           me,
		authEndpoint: authEndpoint,
		store:        store,
		cache:        c,
		ClientID:     defaultClientID,
		RedirectPath: DefaultRedirectPath,
	}
	return ia, nil
}

// getAuthEndpoint fetches the "me" URL to discover the "authorization_endpoint"
func getAuthEndpoint(me string) (string, error) {
	req, err := http.NewRequest("GET", me, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", UserAgent)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", nil
	}
	defer resp.Body.Close()

	// From the spec: "the first HTTP Link header takes precedence"
	for _, l := range link.ParseResponse(resp) {
		if l.Rel == authEndpointRel {
			return l.URI, nil
		}
	}

	data := microformats.Parse(resp.Body, resp.Request.URL)

	authEndpoints := data.Rels[authEndpointRel]
	if len(authEndpoints) == 0 {
		return "", ErrAuthorizationEndpointNotFound
	}
	return authEndpoints[0], nil
}

type verifyResp struct {
	Me    string `json:"me"`
	State string `json:"state"`
	Scope string `json:"scope"`
}

// verifyCode calls the authorization endpoint to verify/authenticate the received code
func (ia *IndieAuth) verifyCode(r *http.Request, code string) (*verifyResp, error) {
	vs := &url.Values{}
	vs.Set("code", code)
	clientID := ia.ClientID(r)
	vs.Set("client_id", clientID)
	vs.Set("redirect_uri", clientID+ia.RedirectPath)

	req, err := http.NewRequest("POST", ia.authEndpoint, strings.NewReader(vs.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", UserAgent)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusForbidden {
		return nil, ErrForbidden
	}
	if resp.StatusCode != http.StatusOK {
		return nil, err
	}
	vresp := &verifyResp{}
	if err := json.NewDecoder(resp.Body).Decode(vresp); err != nil {
		panic(err)
	}
	return vresp, nil
}

// RedirectHandler is a HTTP handler that must be registered on the app at `/indieauth-redirect`
func (ia *IndieAuth) RedirectHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		// Extract query parameters
		q := r.URL.Query()
		me := q.Get("me")
		code := q.Get("code")
		state := q.Get("state")

		// Sanity check
		if me != ia.me {
			panic("invalid me")
		}

		// Verify the state/nonce to protect from XSRF attacks
		p, validState := ia.cache.Get(state)
		if !validState {
			panic(fmt.Errorf("invalid state"))
		}

		// Verify the code against the remote IndieAuth server
		if _, err := ia.verifyCode(r, code); err != nil {
			if err == ErrForbidden {
				w.WriteHeader(http.StatusForbidden)
				return
			}
			panic(err)
		}

		// Update the session
		session, _ := ia.store.Get(r, SessionName)
		session.Values["logged_in"] = true
		session.Save(r, w)

		// Redirect the user to the page requested before the login
		http.Redirect(w, r, p.(string), http.StatusTemporaryRedirect)

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// Redirect responds to the request by redirecting to the authorization endpoint
func (ia *IndieAuth) Redirect(w http.ResponseWriter, r *http.Request) error {
	pu, err := url.Parse(ia.authEndpoint)
	if err != nil {
		return err
	}

	// Generate a random state that will act as a XSRF token
	rawState := make([]byte, 12)
	if _, err := rand.Read(rawState); err != nil {
		return err
	}
	state := fmt.Sprintf("%x", rawState)

	// Store the state in the LRU cache
	ia.cache.Add(state, r.URL.String())

	// Add the query params
	q := pu.Query()
	q.Set("me", ia.me)
	clientID := ia.ClientID(r)
	q.Set("client_id", clientID)
	q.Set("redirect_uri", clientID+ia.RedirectPath)
	q.Set("state", state)
	pu.RawQuery = q.Encode()

	// Do the redirect
	http.Redirect(w, r, pu.String(), http.StatusTemporaryRedirect)
	return nil
}

// Check returns true if there is an existing session with a valid login
func (ia *IndieAuth) Check(r *http.Request) bool {
	// Check if there's a session and if the the user is already logged in
	session, _ := ia.store.Get(r, SessionName)
	loggedIn, ok := session.Values["logged_in"]
	return ok && loggedIn.(bool)
}

// Middleware provides a middleware that will only allow user authenticated against the given IndiAuth endpoint
func (ia *IndieAuth) Middleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != ia.RedirectPath && !ia.Check(r) {
				if err := ia.Redirect(w, r); err != nil {
					if err == ErrForbidden {
						w.WriteHeader(http.StatusForbidden)
						return
					}
					panic(err)
				}
				return
			}

			// The user is already logged in
			next.ServeHTTP(w, r)
			return
		})
	}
}

// Logout logs out the current user
func (ia *IndieAuth) Logout(w http.ResponseWriter, r *http.Request) {
	session, err := ia.store.Get(r, "indieauth")
	if err != nil {
		panic(err)
	}
	session.Values["logged_in"] = false
	session.Save(r, w)
}
