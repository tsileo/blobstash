// Package webauthn implements a single user Webauthn helper
package webauthn // import "a4.io/blobstash/pkg/webauthn"

import (
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
	"github.com/gorilla/mux"
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
	cred  *w.Credential
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

func (u *user) save(rpid string, rcred *w.Credential) error {
	allCreds, err := u.loadAll()
	if err != nil {
		return err
	}
	allCreds = append(allCreds, &credential{rpid, rcred})
	js, err := json.Marshal(allCreds)
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
	web   *w.WebAuthn
	rdata *w.SessionData
	user  *user
}

func New(conf *config.Config, s *session.Session) (*WebAuthn, error) {
	web, err := w.New(&w.Config{
		RPDisplayName: "BlobStash",
		RPID:          "localhost",
		RPOrigin:      "https://localhost",
	})
	if err != nil {
		return nil, err
	}
	cred := &w.Credential{}
	dat, err := ioutil.ReadFile("wa2.json")
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(dat, cred); err != nil {
		return nil, err
	}

	return &WebAuthn{
		sess: s,
		conf: conf,
		web:  web,
		user: &user{conf, cred, nil},
	}, nil
}

func (wa *WebAuthn) BeginRegistration(rw http.ResponseWriter, r *http.Request, origin string) (string, error) {
	web, err := wa.web2(origin)
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
	web, err := wa.web2(origin)
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

func (wa *WebAuthn) RegisterHandler() func(http.ResponseWriter, *http.Request) {
	return func(rw http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "GET":
			dat, err := wa.BeginRegistration(rw, r, "http://localhost:8050/")
			if err != nil {
				panic(err)
			}
			rw.Write([]byte(`<html><a href="#" id="register_link">register</a><script>var credentialCreationOptions = ` + dat + `;
// Base64 to ArrayBuffer
function bufferDecode(value) {
  return Uint8Array.from(atob(value), c => c.charCodeAt(0));
}

// ArrayBuffer to URLBase64
function bufferEncode(value) {
  return btoa(String.fromCharCode.apply(null, new Uint8Array(value)))
	.replace(/\+/g, "-")
	.replace(/\//g, "_")
	.replace(/=/g, "");;
}
credentialCreationOptions.publicKey.challenge = bufferDecode(credentialCreationOptions.publicKey.challenge);
credentialCreationOptions.publicKey.user.id = bufferDecode(credentialCreationOptions.publicKey.user.id);
if (credentialCreationOptions.publicKey.excludeCredentials) {
  for (var i = 0; i < credentialCreationOptions.publicKey.excludeCredentials.length; i++) {
    credentialCreationOptions.publicKey.excludeCredentials[i].id = bufferDecode(credentialCreationOptions.publicKey.excludeCredentials[i].id);
  }
}

document.getElementById("register_link").onclick = function() {
navigator.credentials.create({
  publicKey: credentialCreationOptions.publicKey
}).then(function(credential) {
let attestationObject = credential.response.attestationObject;
let clientDataJSON = credential.response.clientDataJSON;
let rawId = credential.rawId;
let payload = JSON.stringify({
  id: credential.id,
  rawId: bufferEncode(rawId),
  type: credential.type,
    response: {
      attestationObject: bufferEncode(attestationObject),
      clientDataJSON: bufferEncode(clientDataJSON),
	},
});
fetch("/api/webauthn/register", {
  headers: {
    'Accept': 'application/json',
    'Content-Type': 'application/json'
  },
  method: "POST",
  body: payload,
}).then(function(res){ console.log(res) })
  .catch(function(res){ console.log(res) })
})
}
</script></html>`))
		case "POST":
			sessionData, err := wa.getSession(r, "registration")
			if err != nil {
				panic(err)
			}
			credential, err := wa.web.FinishRegistration(wa.user, *sessionData, r)
			if err != nil {
				panic(err)
			}
			wa.user.cred = credential
			cjs, err := json.Marshal(&credential)
			if err != nil {
				panic(err)
			}
			if err := ioutil.WriteFile("wa2.json", cjs, 0600); err != nil {
				panic(err)
			}
		default:
			rw.WriteHeader(http.StatusMethodNotAllowed)
		}
	}
}

func (wa *WebAuthn) web2(origin string) (*w.WebAuthn, error) {
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
	web, err := wa.web2(origin)
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
	web, err := wa.web2(origin)
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
	fmt.Printf("CREDS=%+v\nNCREDS=%+v\n", wa.user.cred, ncred)
	return nil
}

func (wa *WebAuthn) LoginHandler() func(http.ResponseWriter, *http.Request) {
	return func(rw http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "GET":
			options, sessionData, err := wa.web.BeginLogin(wa.user)
			if err != nil {
				panic(err)
			}
			if err := wa.saveSession(rw, r, "login", sessionData); err != nil {
				panic(err)
			}

			js, err := json.Marshal(options)
			if err != nil {
				panic(err)
			}

			rw.Write([]byte(`<html><a href="#" id="login_link">login</a><script>var credentialRequestOptions = ` + string(js) + `;
// Base64 to ArrayBuffer
function bufferDecode(value) {
  return Uint8Array.from(atob(value), c => c.charCodeAt(0));
}

// ArrayBuffer to URLBase64
function bufferEncode(value) {
  return btoa(String.fromCharCode.apply(null, new Uint8Array(value)))
	.replace(/\+/g, "-")
	.replace(/\//g, "_")
	.replace(/=/g, "");;
}
console.log(credentialRequestOptions)
credentialRequestOptions.publicKey.challenge = bufferDecode(credentialRequestOptions.publicKey.challenge);
credentialRequestOptions.publicKey.allowCredentials.forEach(function (listItem) {
  listItem.id = bufferDecode(listItem.id)
});
document.getElementById("login_link").onclick = function() {
navigator.credentials.get({
  publicKey: credentialRequestOptions.publicKey,
}).then(function(assertion) {
console.log(assertion);
let authData = assertion.response.authenticatorData;
let clientDataJSON = assertion.response.clientDataJSON;
let rawId = assertion.rawId;
let sig = assertion.response.signature;
let userHandle = assertion.response.userHandle;
let payload = JSON.stringify({
  id: assertion.id,
  rawId: bufferEncode(rawId),
  type: assertion.type,
  response: {
	authenticatorData: bufferEncode(authData),
	clientDataJSON: bufferEncode(clientDataJSON),
	signature: bufferEncode(sig),
	userHandle: bufferEncode(userHandle),
  },
})
fetch("/api/webauthn/login", {
  headers: {
    'Accept': 'application/json',
    'Content-Type': 'application/json'
  },
  method: "POST",
  body: payload,
}).then(function(res){ console.log("YES"); console.log(res) })
  .catch(function(res){ console.log(res) })
})
}
</script></html>`))
		case "POST":
			sessionData, err := wa.getSession(r, "login")
			if err != nil {
				panic(err)
			}
			ncred, err := wa.web.FinishLogin(wa.user, *sessionData, r)
			if err != nil {
				panic(err)
			}
			fmt.Printf("CREDS=%+v\nNCREDS=%+v\n", wa.user.cred, ncred)
		default:
			rw.WriteHeader(http.StatusMethodNotAllowed)
		}
	}
}

func (wa *WebAuthn) Register(r *mux.Router, basicAuth func(http.Handler) http.Handler) {
	r.Handle("/register", http.HandlerFunc(wa.RegisterHandler()))
	r.Handle("/login", http.HandlerFunc(wa.LoginHandler()))
}
