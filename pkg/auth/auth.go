package auth // import "a4.io/blobstash/pkg/auth"

import (
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"net/http"
	"strconv"

	"a4.io/blobstash/pkg/config"
	"a4.io/blobstash/pkg/httputil"
	"a4.io/blobstash/pkg/perms"

	gcontext "github.com/gorilla/context"
	log "github.com/inconshreveable/log15"
	"github.com/zpatrick/rbac"
)

type key int

const authKey key = 0

var auths = []*Auth{}
var logger log.Logger

type Auth struct {
	ID       string
	roles    rbac.Roles
	Username string
	Password string
	encoded  []byte
	sroles   []string
}

func Setup(conf *config.Config, l log.Logger) error {
	if err := perms.Setup(conf); err != nil {
		return err
	}
	logger = l
	for _, c := range conf.Auth {
		roles, err := perms.GetRoles(c.Roles)
		if err != nil {
			return err
		}
		encoded := "Basic " + base64.StdEncoding.EncodeToString([]byte(c.Username+":"+c.Password))
		auths = append(auths, &Auth{
			ID:       c.ID,
			roles:    roles,
			sroles:   c.Roles,
			Username: c.Username,
			Password: c.Password,
			encoded:  []byte(encoded),
		})
	}
	return nil
}

func Check(req *http.Request) bool {
	h := req.Header.Get("Authorization")
	for _, auth := range auths {
		if subtle.ConstantTimeCompare([]byte(h), auth.encoded) == 1 {
			logger.Debug("successful auth", "auth", auth.ID, "roles", auth.sroles)
			gcontext.Set(req, authKey, auth)
			return true
		}
	}
	return false
}

func Can(w http.ResponseWriter, r *http.Request, action, resource string) bool {
	auth, ok := gcontext.GetOk(r, authKey)
	if !ok {
		// If there's no auth, it's not enabled
		return true
	}
	a := auth.(*Auth)
	can, err := a.roles.Can(action, resource)
	if err != nil {
		panic(err)
	}
	w.Header().Set("BlobStash-Auth-ID", a.ID)
	w.Header().Set("BlobStash-Auth-Success", strconv.FormatBool(can))
	w.Header().Set("BlobStash-RBAC-Action", action)
	w.Header().Set("BlobStash-RBAC-Resource", resource)

	logger.Debug(fmt.Sprintf("check=%v", can), "auth", a.ID, "roles", a.sroles, "requested_action", action, "requested_resource", resource)
	return can
}

func Forbidden(w http.ResponseWriter) {
	httputil.WriteJSONError(w, http.StatusForbidden, http.StatusText(http.StatusForbidden))
}
