package permissions

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/tsileo/blobstash/config/pathutil"
	"github.com/tsileo/blobstash/httputil"
	serverMiddleware "github.com/tsileo/blobstash/middleware"

	"github.com/GeertJohan/yubigo"
	"github.com/gorilla/context"
	"github.com/gorilla/mux"
	log2 "gopkg.in/inconshreveable/log15.v2"
)

// TODO(tsileo): add yubikey support with a POST endpoint to retrive the attached API key (add a yubiIndex)
// TODO(tsileo): add a way to delete an API key
// TODO(tsileo): a way to check custom permissions in the Lua module

func randomKey() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.URLEncoding.EncodeToString(b), nil
}

// Key holds the API key key along with metadata
type Key struct {
	Name        string   `json:"name"`
	YubkikeyID  string   `json:"yubikey_id,omitempty"`
	Permissions []string `json:"permissions"`
	Key         string   `json:"key"`

	isAdmin    bool                `json:"-"`
	permsIndex map[string]struct{} `json:"-"`
	log        log2.Logger         `json:"-"`
}

// IsAdmin returns true if the Key has admin permission
func (k *Key) IsAdmin() bool {
	return k.isAdmin
}

// CheckPerms check if the key contains at least one the required perms
func (k *Key) CheckPerms(perms ...string) bool {
	if k.isAdmin {
		return true
	}
	for _, p := range perms {
		if _, ok := k.permsIndex[p]; ok {
			return true
		}
	}
	return false
}

// Permissions is the module responsible for handling the permissions of the different APIs
type Permissions struct {
	log      log2.Logger
	yubiAuth *yubigo.YubiAuth

	Keys          map[string]*Key
	KeysByName    map[string]*Key
	KeysByYubikey map[string]*Key
}

func New(log log2.Logger, conf map[string]interface{}) *Permissions {
	// Check if the Yubico API config is present
	var err error
	var yubiAuth *yubigo.YubiAuth
	var yubicoApiKey, yubicoApiID string
	if apiKey, ok := conf["yubico_api_key"]; ok {
		yubicoApiKey = apiKey.(string)
	}
	if apiID, ok := conf["yubico_api_id"]; ok {
		yubicoApiID = apiID.(string)
	}
	if yubicoApiKey != "" {
		yubiAuth, err = yubigo.NewYubiAuth(yubicoApiID, yubicoApiKey)
		if err != nil {
			panic(err)
		}
	}

	return &Permissions{
		log:           log,
		yubiAuth:      yubiAuth,
		Keys:          map[string]*Key{},
		KeysByName:    map[string]*Key{},
		KeysByYubikey: map[string]*Key{},
	}
}

// Load all the *.key files in the config dir
func (p *Permissions) Load() error {
	// Load all the keys present in the config dir
	keys, err := filepath.Glob(filepath.Join(pathutil.ConfigDir(), "*.key"))
	if err != nil {
		return err
	}
	// FIXME(tsileo): generate an API key at startup if there's none
	for _, keyFile := range keys {
		if strings.HasSuffix(keyFile, "api.key") || strings.HasSuffix(keyFile, "hawk.key") {
			continue
		}
		f, err := os.Open(keyFile)
		if err != nil {
			return err
		}
		defer f.Close()
		key := &Key{}
		if err := json.NewDecoder(f).Decode(key); err != nil {
			return err
		}
		key.log = p.log
		key.permsIndex = map[string]struct{}{}
		for _, p := range key.Permissions {
			if p == "admin" {
				key.isAdmin = true
			}
			key.permsIndex[p] = struct{}{}
		}
		p.Keys[key.Key] = key
		p.KeysByName[key.Name] = key
		if key.YubkikeyID != "" {
			p.KeysByYubikey[key.YubkikeyID] = key
		}
	}
	return nil
}

func (p *Permissions) AuthFunc(username string, password string, r *http.Request) bool {
	if k, ok := p.Keys[password]; ok {
		p.log.Debug("matched key", "key_name", k.Name)
		context.Set(r, "key", k)
		return true
	}
	return false
}

func (p *Permissions) RegisterRoute(r *mux.Router, middlewares *serverMiddleware.SharedMiddleware) {
	r.Handle("/", middlewares.Auth(http.HandlerFunc(p.indexHandler())))
	r.Handle("/otp", middlewares.Auth(http.HandlerFunc(p.otpHandler())))
}

func (p *Permissions) indexHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "GET":
			var k *Key
			if rv := context.Get(r, "key"); rv != nil {
				k = rv.(*Key)
			}
			httputil.WriteJSON(w, map[string]interface{}{
				"permissions": k.Permissions,
				"name":        k.Name,
				"yubikey_id":  k.YubkikeyID,
			})
		case "POST":
			CheckPerms(r, "admin")
			k := &Key{}
			defer r.Body.Close()
			if err := json.NewDecoder(r.Body).Decode(k); err != nil {
				panic(err)
			}

			// Generate a new random API key
			key, err := randomKey()
			if err != nil {
				panic(err)
			}
			k.Key = key

			// Attach the logger
			k.log = p.log

			// Ensure there's a name
			if k.Name == "" {
				panic("missing key name")
			}

			// Save the key in memory
			p.Keys[k.Key] = k
			p.KeysByName[k.Name] = k
			if k.YubkikeyID != "" {
				p.KeysByYubikey[k.YubkikeyID] = k
			}

			// Save the API key in a file
			kpath := filepath.Join(pathutil.ConfigDir(), fmt.Sprintf("%s.key", k.Name))
			p.log.Debug(fmt.Sprintf("writing new API key at %s", kpath))
			js, err := json.Marshal(k)
			if err != nil {
				panic(err)
			}
			if err := ioutil.WriteFile(kpath, js, 0644); err != nil {
				panic(err)
			}

			// Out the newly created key
			p.log.Info("new API key saved", "name", k.Name)
			httputil.WriteJSON(w, k)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}
}

type OTPInput struct {
	OTP string `json:"otp"`
}

func (p *Permissions) otpHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "POST":
			k := &OTPInput{}
			defer r.Body.Close()
			if err := json.NewDecoder(r.Body).Decode(k); err != nil {
				panic(err)
			}
			if p.yubiAuth == nil {
				httputil.WriteJSONError(w, http.StatusInternalServerError, "missing Yubico API config")
				return
			}
			if len(k.OTP) != 44 {
				httputil.WriteJSONError(w, http.StatusInternalServerError, "OTP has wrong length")
				return
			}

			yubikeyID := k.OTP[0:12]
			var key *Key
			var ok bool
			key, ok = p.KeysByYubikey[yubikeyID]
			if !ok {
				httputil.WriteJSONError(w, http.StatusUnauthorized, "Unknown Yubikey ID")
				return
			}

			result, ok, err := p.yubiAuth.Verify(k.OTP)
			if err != nil {
				panic(&httputil.PublicError{err})
			}
			p.log.Info("otp request", "yubikey ID", yubikeyID, "result", result, "ok", ok)
			httputil.WriteJSON(w, key)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}
}

// CheckPerms panic if the current Key don't have one of the given permissions
func CheckPerms(r *http.Request, perms ...string) {
	var k *Key
	if rv := context.Get(r, "key"); rv != nil {
		k = rv.(*Key)
	}
	if !k.CheckPerms(perms...) {
		panic(&PermError{perms})
	}
}

// PermError is an error that is displayable by the RecoverHandler
// and will output a 401 status code
type PermError struct {
	Requested []string
}

// Error implements the Error interface
func (pe *PermError) Error() string {
	return fmt.Sprintf("Missing one of the following permissions: %s", strings.Join(pe.Requested, ", "))
}

// Status implements the httputil.StatusErrorer interface
func (pe *PermError) Status() int {
	return http.StatusUnauthorized
}
