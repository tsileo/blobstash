// Package webauthn implements a single user Webauthn helper
package webauthn // import "a4.io/blobstash/pkg/webauthn"

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"

	"a4.io/blobstash/pkg/config"
	"a4.io/blobstash/pkg/session"
	"github.com/e3b0c442/warp"
	lua "github.com/yuin/gopher-lua"
)

var id = []byte("1")
var name = "admin"
var sessionName = "webauthn"

type rp struct {
	origin string
}

func (r rp) EntityID() string {
	u, _ := url.Parse(r.origin)
	return u.Hostname()
}

func (r rp) EntityName() string {
	return r.origin
}

func (r rp) EntityIcon() string {
	return ""
}

func (r rp) Origin() string {
	return r.origin
}

type user struct {
	conf        *config.Config
	name        string
	id          []byte
	credentials map[string]warp.Credential
}

func (u *user) EntityID() []byte {
	return u.id
}

func (u *user) EntityName() string {
	return u.name
}

func (u *user) EntityDisplayName() string {
	return u.name
}

func (u *user) EntityIcon() string {
	return ""
}

func (u *user) Credentials() map[string]warp.Credential {
	return u.credentials
}

type credential struct {
	owner warp.User
	Att   *warp.AttestationObject
	RPID  string
}

func (c *credential) Owner() warp.User {
	return c.owner
}

func (c *credential) CredentialID() []byte {
	return c.Att.AuthData.AttestedCredentialData.CredentialID
}

func (c *credential) CredentialPublicKey() []byte {

	return c.Att.AuthData.AttestedCredentialData.CredentialPublicKey
}

func (c *credential) CredentialSignCount() uint {
	return uint(c.Att.AuthData.SignCount)
}

type sessionData struct {
	CreationOptions *warp.PublicKeyCredentialCreationOptions
	RequestOptions  *warp.PublicKeyCredentialRequestOptions
}

func loadAll(conf *config.Config) ([]*credential, error) {
	allCreds := []*credential{}
	dat, err := ioutil.ReadFile(filepath.Join(conf.VarDir(), "webauthn.json"))
	switch {
	case err == nil:
		if err := json.Unmarshal(dat, &allCreds); err != nil {
			return nil, err
		}
		return allCreds, nil
	case os.IsNotExist(err):
		return allCreds, nil
	default:
		return nil, err
	}
}

func (u *user) load(rpid string) error {
	u.credentials = map[string]warp.Credential{}
	allCreds, err := loadAll(u.conf)
	if err != nil {
		return err
	}
	for _, cred := range allCreds {
		if cred.RPID == rpid {
			id := base64.RawURLEncoding.EncodeToString(cred.Att.AuthData.AttestedCredentialData.CredentialID)
			u.credentials[id] = cred
		}
	}
	return nil
}

// save or update a Webauthn credential in the JSON DB file
func (u *user) save(rpid string, rcred *credential, cid []byte, authData *warp.AuthenticatorData) error {
	allCreds, err := loadAll(u.conf)
	if err != nil {
		return err
	}
	newCreds := []*credential{}
	for _, acred := range allCreds {
		if acred.RPID == rpid && authData != nil && bytes.Equal(acred.Att.AuthData.AttestedCredentialData.CredentialID, cid) {
			acred.Att.AuthData.SignCount = authData.SignCount
		}

		newCreds = append(newCreds, acred)
	}

	if rcred != nil {
		newCreds = append(newCreds, rcred)
	}

	js, err := json.Marshal(newCreds)
	if err != nil {
		return err
	}
	if err := ioutil.WriteFile(filepath.Join(u.conf.VarDir(), "webauthn.json"), js, 0600); err != nil {
		return err
	}
	return nil
}

type WebAuthn struct {
	conf *config.Config
	sess *session.Session
	user *user
}

func New(conf *config.Config, s *session.Session) (*WebAuthn, error) {
	return &WebAuthn{
		sess: s,
		conf: conf,
		user: &user{
			conf:        conf,
			id:          id,
			name:        name,
			credentials: map[string]warp.Credential{},
		},
	}, nil
}

func (wa *WebAuthn) findCredential(id []byte) (warp.Credential, error) {
	strID := base64.RawStdEncoding.EncodeToString(id)
	if c, ok := wa.user.credentials[strID]; ok {
		return c, nil
	}
	return nil, fmt.Errorf("no credential")
}

func (wa *WebAuthn) BeginRegistration(rw http.ResponseWriter, r *http.Request, origin string) (string, error) {
	relyingParty := &rp{
		origin: origin,
	}

	if err := wa.user.load(relyingParty.EntityID()); err != nil {
		return "", err
	}

	opts, err := warp.StartRegistration(relyingParty, wa.user, warp.Attestation(warp.ConveyanceDirect))
	if err != nil {
		return "", err
	}

	sessionData := &sessionData{
		CreationOptions: opts,
	}

	if err := wa.saveSession(rw, r, "registration", sessionData); err != nil {
		return "", err
	}

	js, err := json.Marshal(opts)
	if err != nil {
		return "", err
	}

	return string(js), nil
}

func (wa *WebAuthn) CredentialFinder(id []byte) (warp.Credential, error) {
	return nil, fmt.Errorf("no creds")
}

func (wa *WebAuthn) FinishRegistration(rw http.ResponseWriter, r *http.Request, origin, js string) error {
	relyingParty := &rp{
		origin: origin,
	}

	if err := wa.user.load(relyingParty.EntityID()); err != nil {
		return err
	}

	sessionData, err := wa.getSession(r, "registration")
	if err != nil {
		panic(fmt.Errorf("failed to get session: %w", err))
	}

	cred := warp.AttestationPublicKeyCredential{}
	if err := json.Unmarshal([]byte(js), &cred); err != nil {
		return fmt.Errorf("failed to unmarshal attestation: %w", err)
	}

	att, err := warp.FinishRegistration(relyingParty, wa.CredentialFinder, sessionData.CreationOptions, &cred)
	if err != nil {
		for err != nil {
			fmt.Printf("%v", err)
			err = errors.Unwrap(err)
		}
		return fmt.Errorf("failed to finish registration: %w", err)
	}

	fmt.Printf("att=%+v\n\n", att)

	newCred := &credential{
		RPID:  relyingParty.EntityID(),
		Att:   att,
		owner: wa.user,
	}

	if err := wa.user.save(relyingParty.EntityID(), newCred, nil, nil); err != nil {
		panic(err)
	}

	return nil
}

func (wa *WebAuthn) saveSession(rw http.ResponseWriter, r *http.Request, name string, sessionData *sessionData) error {
	fmt.Printf("WA: %+v\n", wa)
	store, err := wa.sess.Session().Get(r, "webauthn")
	if err != nil {
		return err
	}
	jsession, err := json.Marshal(sessionData)
	if err != nil {
		return err
	}

	store.Values[name] = jsession

	if err := store.Save(r, rw); err != nil {
		return err
	}
	return nil
}

func (wa *WebAuthn) getSession(r *http.Request, name string) (*sessionData, error) {
	store, err := wa.sess.Session().Get(r, "webauthn")
	if err != nil {
		return nil, err
	}
	sessionData := &sessionData{}
	js := store.Values[name].([]byte)

	if err := json.Unmarshal(js, &sessionData); err != nil {
		return nil, err
	}
	delete(store.Values, name)
	return sessionData, nil
}

func (wa *WebAuthn) BeginLogin(rw http.ResponseWriter, r *http.Request, origin string) (string, error) {
	relyingParty := &rp{
		origin: origin,
	}

	if err := wa.user.load(relyingParty.EntityID()); err != nil {
		return "", err
	}

	opts, err := warp.StartAuthentication(warp.AllowCredentials(
		func(user warp.User) []warp.PublicKeyCredentialDescriptor {
			ds := []warp.PublicKeyCredentialDescriptor{}
			for _, c := range user.Credentials() {
				ds = append(ds, warp.PublicKeyCredentialDescriptor{
					Type: "public-key",
					ID:   c.CredentialID(),
				})
			}
			return ds
		}(wa.user)),
		warp.RelyingPartyID(relyingParty.EntityID()),
	)
	sessionData := &sessionData{
		RequestOptions: opts,
	}

	if err := wa.saveSession(rw, r, "login", sessionData); err != nil {
		return "", err
	}

	js, err := json.Marshal(opts)
	if err != nil {
		return "", err
	}
	return string(js), nil
}

func (wa *WebAuthn) FinishLogin(rw http.ResponseWriter, r *http.Request, origin, js string) error {
	relyingParty := &rp{
		origin: origin,
	}

	if err := wa.user.load(relyingParty.EntityID()); err != nil {
		return err
	}

	sessionData, err := wa.getSession(r, "login")
	if err != nil {
		panic(err)
	}

	cred := warp.AssertionPublicKeyCredential{}
	if err := json.Unmarshal([]byte(js), &cred); err != nil {
		return err
	}

	newAuthData, err := warp.FinishAuthentication(
		relyingParty,
		func(_ []byte) (warp.User, error) {
			return wa.user, nil
		},
		sessionData.RequestOptions,
		&cred,
	)

	if err != nil {
		return err
	}

	if err := wa.user.save(relyingParty.EntityID(), nil, cred.RawID, newAuthData); err != nil {
		panic(err)
	}

	return nil
}

func (wa *WebAuthn) SetupLua(L *lua.LState, baseURL string, w http.ResponseWriter, r *http.Request) {
	L.PreloadModule("webauthn", func(L *lua.LState) int {
		u, err := url.Parse(baseURL)
		if err != nil {
			panic(err)
		}
		u.Path = ""
		baseURL = u.String()

		mod := L.SetFuncs(L.NewTable(), map[string]lua.LGFunction{
			"registered_credentials": func(L *lua.LState) int {
				relyingParty := &rp{
					origin: baseURL,
				}

				if err := wa.user.load(relyingParty.EntityID()); err != nil {
					panic(err)
				}
				tbl := L.NewTable()

				for id, _ := range wa.user.credentials {
					tbl.Append(lua.LString(id))
				}

				L.Push(tbl)
				return 1
			},
			"begin_registration": func(L *lua.LState) int {
				js, err := wa.BeginRegistration(w, r, baseURL)
				if err != nil {
					panic(err)
				}
				L.Push(lua.LString(js))
				return 1
			},
			"finish_registration": func(L *lua.LState) int {
				if err := wa.FinishRegistration(w, r, baseURL, L.ToString(1)); err != nil {
					panic(err)
				}
				L.Push(lua.LNil)
				return 1
			},
			"begin_login": func(L *lua.LState) int {
				js, err := wa.BeginLogin(w, r, baseURL)
				if err != nil {
					panic(err)
				}
				L.Push(lua.LString(js))
				return 1
			},
			"finish_login": func(L *lua.LState) int {
				js := L.ToString(1)
				if err := wa.FinishLogin(w, r, baseURL, js); err != nil {
					panic(err)
				}
				L.Push(lua.LNil)
				return 1
			},
		})
		// returns the module
		L.Push(mod)
		return 1
	})
}
