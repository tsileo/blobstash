// Package webauthn implements a single user Webauthn helper
package webauthn // import "a4.io/blobstash/pkg/webauthn"

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"a4.io/blobstash/pkg/config"
	"a4.io/blobstash/pkg/session"
	"github.com/duo-labs/webauthn/protocol"
	w "github.com/duo-labs/webauthn/webauthn"
	lua "github.com/yuin/gopher-lua"
)

var id = []byte("1")
var name = "admin"
var sessionName = "webauthn"

type credential struct {
	RPID string
	Cred *w.Credential
}

type user struct {
	conf  *config.Config
	creds []w.Credential
}

func (u *user) loadAll() ([]*credential, error) {
	allCreds := []*credential{}
	dat, err := ioutil.ReadFile(filepath.Join(u.conf.VarDir(), "webauthn.json"))
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
	u.creds = nil
	allCreds, err := u.loadAll()
	if err != nil {
		return err
	}
	fmt.Printf("ALLCREDS=%+v\n", allCreds)
	for _, cred := range allCreds {
		if cred.RPID == rpid {
			u.creds = append(u.creds, *cred.Cred)
		}
	}
	fmt.Printf("ALLCREDS=%+v\n", u.creds)
	return nil
}

// save or update a Webauthn credential in the JSON DB file
func (u *user) save(rpid string, rcred *w.Credential) error {
	allCreds, err := u.loadAll()
	if err != nil {
		return err
	}
	newCreds := []*credential{}
	var replaced bool
	for _, acred := range allCreds {
		if acred.RPID == rpid && bytes.Equal(acred.Cred.ID, rcred.ID) {
			newCreds = append(newCreds, &credential{rpid, rcred})
			replaced = true
			continue
		}

		newCreds = append(newCreds, acred)
	}

	if !replaced {
		newCreds = append(newCreds, &credential{rpid, rcred})
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

func (u *user) WebAuthnID() []byte {
	return id
}

func (u *user) WebAuthnName() string {
	return name
}

func (u *user) WebAuthnDisplayName() string {
	return name
}

func (u *user) WebAuthnIcon() string {
	return ""
}

func (u *user) CredentialExcludeList() []protocol.CredentialDescriptor {
	credentialExcludeList := []protocol.CredentialDescriptor{}
	//for _, cred := range u.credentials {
	//	descriptor := protocol.CredentialDescriptor{
	//		Type:         protocol.PublicKeyCredentialType,
	//		CredentialID: cred.ID,
	//	}
	//	credentialExcludeList = append(credentialExcludeList, descriptor)
	//}

	return credentialExcludeList
}

func (u *user) WebAuthnCredentials() []w.Credential {
	return u.creds
}

type WebAuthn struct {
	conf  *config.Config
	sess  *session.Session
	rdata *w.SessionData
	user  *user
}

func New(conf *config.Config, s *session.Session) (*WebAuthn, error) {
	return &WebAuthn{
		sess: s,
		conf: conf,
		user: &user{conf, nil},
	}, nil
}

func (wa *WebAuthn) BeginRegistration(rw http.ResponseWriter, r *http.Request, origin string) (string, error) {
	web, err := wa.web(origin)
	if err != nil {
		return "", err
	}

	registerOptions := func(credCreationOpts *protocol.PublicKeyCredentialCreationOptions) {

	}
	options, sessionData, err := web.BeginRegistration(
		wa.user,
		registerOptions,
	)
	if err != nil {
		return "", err
	}

	if err := wa.saveSession(rw, r, "registration", sessionData); err != nil {
		return "", err
	}

	js, err := json.Marshal(options)
	if err != nil {
		return "", err
	}

	return string(js), nil
}

func (wa *WebAuthn) FinishRegistration(rw http.ResponseWriter, r *http.Request, origin, js string) error {
	web, err := wa.web(origin)
	if err != nil {
		return err
	}

	sessionData, err := wa.getSession(r, "registration")
	if err != nil {
		panic(err)
	}
	fmt.Printf("JS=%s\n\n\n\n", js)
	inputPayload := strings.NewReader(js)
	parsedResponse, err := protocol.ParseCredentialCreationResponseBody(inputPayload)
	fmt.Printf("RESP=%+v\nerr=%v\nuser=%+v\n\n", parsedResponse, err, wa.user)
	ncred, err := web.CreateCredential(wa.user, *sessionData, parsedResponse)
	if err != nil {
		return err
	}
	fmt.Printf("ncred=%v\n", ncred)
	if err := wa.user.save(web.Config.RPID, ncred); err != nil {
		return err
	}
	return nil
}

func (wa *WebAuthn) saveSession(rw http.ResponseWriter, r *http.Request, name string, sessionData *w.SessionData) error {
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

func (wa *WebAuthn) getSession(r *http.Request, name string) (*w.SessionData, error) {
	store, err := wa.sess.Session().Get(r, "webauthn")
	if err != nil {
		return nil, err
	}
	sessionData := w.SessionData{}
	js := store.Values[name].([]byte)

	if err := json.Unmarshal(js, &sessionData); err != nil {
		return nil, err
	}
	delete(store.Values, name)
	return &sessionData, nil
}

func (wa *WebAuthn) web(origin string) (*w.WebAuthn, error) {
	u, err := url.Parse(origin)
	if err != nil {
		return nil, err
	}
	rpid := strings.Split(u.Host, ":")[0]
	if rpid == "0.0.0.0" {
		rpid = "localhost"
	}
	u.Path = "/"
	u.Host = strings.Replace(u.Host, "0.0.0.0", "localhost", 1)

	web, err := w.New(&w.Config{
		RPDisplayName: "BlobStash",
		RPID:          rpid,
		RPOrigin:      u.String(),
	})
	if err != nil {
		return nil, err
	}

	return web, nil
}

func (wa *WebAuthn) BeginLogin(rw http.ResponseWriter, r *http.Request, origin string) (string, error) {
	web, err := wa.web(origin)
	if err != nil {
		return "", err
	}
	if err := wa.user.load(web.Config.RPID); err != nil {
		panic(err)
	}
	options, sessionData, err := web.BeginLogin(wa.user)
	if err != nil {
		return "", err
	}
	if err := wa.saveSession(rw, r, "login", sessionData); err != nil {
		return "", err
	}

	js, err := json.Marshal(options)
	if err != nil {
		return "", err
	}
	return string(js), nil
}

func (wa *WebAuthn) FinishLogin(rw http.ResponseWriter, r *http.Request, origin, js string) error {
	web, err := wa.web(origin)
	if err != nil {
		return err
	}
	sessionData, err := wa.getSession(r, "login")
	if err != nil {
		return err
	}
	inputPayload := strings.NewReader(js)
	parsedResponse, err := protocol.ParseCredentialRequestResponseBody(inputPayload)
	if err != nil {
		return err
	}
	ncred, err := web.ValidateLogin(wa.user, *sessionData, parsedResponse)
	if err != nil {
		return err
	}
	// FIXME(ts): check the counter, warning, and store the new cred
	if err := wa.user.save(web.Config.RPID, ncred); err != nil {
		return err
	}
	fmt.Printf("NCREDS_TO_SAVE=%+v\n", ncred)
	return nil
}

func (wa *WebAuthn) SetupLua(L *lua.LState, baseURL string, w http.ResponseWriter, r *http.Request) {
	L.PreloadModule("webauthn", func(L *lua.LState) int {
		mod := L.SetFuncs(L.NewTable(), map[string]lua.LGFunction{
			"registered_credentials": func(L *lua.LState) int {
				web, err := wa.web(baseURL)
				if err != nil {
					panic(err)
				}

				if err := wa.user.load(web.Config.RPID); err != nil {
					panic(err)
				}

				tbl := L.NewTable()
				if wa.user.creds != nil {
					for _, cred := range wa.user.creds {
						tbl.Append(lua.LString(string(cred.ID)))
					}
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
