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

type user2 struct {
	conf        *config.Config
	name        string
	id          []byte
	credentials map[string]warp.Credential
}

func (u *user2) EntityID() []byte {
	return u.id
}

func (u *user2) EntityName() string {
	return u.name
}

func (u *user2) EntityDisplayName() string {
	return u.name
}

func (u *user2) EntityIcon() string {
	return ""
}

func (u *user2) Credentials() map[string]warp.Credential {
	return u.credentials
}

type credential2 struct {
	owner warp.User
	Att   *warp.AttestationObject
	RPID  string
}

func (c *credential2) Owner() warp.User {
	return c.owner
}

func (c *credential2) CredentialID() []byte {
	return c.Att.AuthData.AttestedCredentialData.CredentialID
}

func (c *credential2) CredentialPublicKey() []byte {

	return c.Att.AuthData.AttestedCredentialData.CredentialPublicKey
}

func (c *credential2) CredentialSignCount() uint {
	return 0
}

type sessionData struct {
	CreationOptions *warp.PublicKeyCredentialCreationOptions
	RequestOptions  *warp.PublicKeyCredentialRequestOptions
}

func loadAll(conf *config.Config) ([]*credential2, error) {
	allCreds := []*credential2{}
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

func (u *user2) load(rpid string) error {
	u.credentials = map[string]warp.Credential{}
	allCreds, err := loadAll(u.conf)
	if err != nil {
		return err
	}
	fmt.Printf("ALLCREDS=%+v\n", allCreds)
	for _, cred := range allCreds {
		if cred.RPID == rpid {
			id := base64.RawURLEncoding.EncodeToString(cred.Att.AuthData.AttestedCredentialData.CredentialID)
			u.credentials[id] = cred
		}
	}
	return nil
}

// save or update a Webauthn credential in the JSON DB file
func (u *user2) save(rpid string, rcred *credential2) error {
	allCreds, err := loadAll(u.conf)
	if err != nil {
		return err
	}
	newCreds := []*credential2{}
	var replaced bool
	for _, acred := range allCreds {
		if acred.RPID == rpid && bytes.Equal(acred.Att.AuthData.AttestedCredentialData.CredentialID, rcred.Att.AuthData.AttestedCredentialData.CredentialID) {
			newCreds = append(newCreds, rcred)
			replaced = true
			continue
		}

		newCreds = append(newCreds, acred)
	}

	if !replaced {
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
	conf  *config.Config
	sess  *session.Session
	user2 *user2
}

func New(conf *config.Config, s *session.Session) (*WebAuthn, error) {
	return &WebAuthn{
		sess: s,
		conf: conf,
		user2: &user2{
			conf:        conf,
			id:          id,
			name:        name,
			credentials: map[string]warp.Credential{},
		},
	}, nil
}

func (wa *WebAuthn) findCredential(id []byte) (warp.Credential, error) {
	strID := base64.RawStdEncoding.EncodeToString(id)
	if c, ok := wa.user2.credentials[strID]; ok {
		return c, nil
	}
	return nil, fmt.Errorf("no credential")
}

func (wa *WebAuthn) BeginRegistration2(rw http.ResponseWriter, r *http.Request, origin string) (string, error) {
	relyingParty := &rp{
		origin: origin,
	}

	if err := wa.user2.load(relyingParty.EntityID()); err != nil {
		return "", err
	}

	opts, err := warp.StartRegistration(relyingParty, wa.user2, warp.Attestation(warp.ConveyanceDirect))
	if err != nil {
		return "", err
	}

	sessionData := &sessionData{
		CreationOptions: opts,
	}

	if err := wa.saveSession2(rw, r, "registration", sessionData); err != nil {
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

func (wa *WebAuthn) FinishRegistration2(rw http.ResponseWriter, r *http.Request, origin, js string) error {
	relyingParty := &rp{
		origin: origin,
	}

	if err := wa.user2.load(relyingParty.EntityID()); err != nil {
		return err
	}

	fmt.Printf("will GOT SESSION\n\n")
	sessionData, err := wa.getSession2(r, "registration")
	if err != nil {
		panic(fmt.Errorf("failed to get session: %w", err))
	}

	fmt.Printf("GOT SESSION\n\n")

	cred := warp.AttestationPublicKeyCredential{}
	if err := json.Unmarshal([]byte(js), &cred); err != nil {
		return fmt.Errorf("failed to unmarshal attestation: %w", err)
	}

	att, err := warp.FinishRegistration(relyingParty, wa.CredentialFinder, sessionData.CreationOptions, &cred)
	if err != nil {
		fmt.Printf("finish reg failed: %v\n\n", err)
		for err != nil {
			fmt.Printf("%v", err)
			err = errors.Unwrap(err)
		}
		return fmt.Errorf("failed to finish registration: %w", err)
	}

	fmt.Printf("att=%+v\n\n", att)

	newCred := &credential2{
		RPID:  relyingParty.EntityID(),
		Att:   att,
		owner: wa.user2,
	}

	if err := wa.user2.save(relyingParty.EntityID(), newCred); err != nil {
		panic(err)
	}

	return nil
}

func (wa *WebAuthn) saveSession2(rw http.ResponseWriter, r *http.Request, name string, sessionData *sessionData) error {
	fmt.Printf("WA: %+v\n", wa)
	store, err := wa.sess.Session().Get(r, "webauthn2")
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

func (wa *WebAuthn) getSession2(r *http.Request, name string) (*sessionData, error) {
	store, err := wa.sess.Session().Get(r, "webauthn2")
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

func (wa *WebAuthn) BeginLogin2(rw http.ResponseWriter, r *http.Request, origin string) (string, error) {
	relyingParty := &rp{
		origin: origin,
	}

	if err := wa.user2.load(relyingParty.EntityID()); err != nil {
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
		}(wa.user2)),
		warp.RelyingPartyID(relyingParty.EntityID()),
	)
	sessionData := &sessionData{
		RequestOptions: opts,
	}

	if err := wa.saveSession2(rw, r, "login3", sessionData); err != nil {
		return "", err
	}

	js, err := json.Marshal(opts)
	if err != nil {
		return "", err
	}
	return string(js), nil
}

func (wa *WebAuthn) FinishLogin2(rw http.ResponseWriter, r *http.Request, origin, js string) error {
	relyingParty := &rp{
		origin: origin,
	}

	if err := wa.user2.load(relyingParty.EntityID()); err != nil {
		return err
	}

	sessionData, err := wa.getSession2(r, "login3")
	if err != nil {
		panic(err)
	}

	cred := warp.AssertionPublicKeyCredential{}
	if err := json.Unmarshal([]byte(js), &cred); err != nil {
		return err
	}

	_, err = warp.FinishAuthentication(
		relyingParty,
		func(_ []byte) (warp.User, error) {
			return wa.user2, nil
		},
		sessionData.RequestOptions,
		&cred,
	)

	if err != nil {
		return err
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

				if err := wa.user2.load(relyingParty.EntityID()); err != nil {
					panic(err)
				}
				tbl := L.NewTable()

				for id, _ := range wa.user2.credentials {
					tbl.Append(lua.LString(id))
				}

				L.Push(tbl)
				return 1
			},
			"begin_registration": func(L *lua.LState) int {
				js, err := wa.BeginRegistration2(w, r, baseURL)
				if err != nil {
					panic(err)
				}
				L.Push(lua.LString(js))
				return 1
			},
			"finish_registration": func(L *lua.LState) int {
				if err := wa.FinishRegistration2(w, r, baseURL, L.ToString(1)); err != nil {
					panic(err)
				}
				L.Push(lua.LNil)
				return 1
			},
			"begin_login": func(L *lua.LState) int {
				js, err := wa.BeginLogin2(w, r, baseURL)
				if err != nil {
					panic(err)
				}
				L.Push(lua.LString(js))
				return 1
			},
			"finish_login": func(L *lua.LState) int {
				js := L.ToString(1)
				if err := wa.FinishLogin2(w, r, baseURL, js); err != nil {
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
