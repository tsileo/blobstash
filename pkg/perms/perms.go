package perms // import "a4.io/blobstash/pkg/perms"

import (
	"fmt"
	"strings"

	"a4.io/blobstash/pkg/config"
	"github.com/zpatrick/rbac"
)

type ActionType string
type ObjectType string
type ServiceName string

// Actions
const (
	Read     ActionType = "read"
	Stat     ActionType = "stat"
	Write    ActionType = "write"
	List     ActionType = "list"
	Snapshot ActionType = "snapshot"
	GC       ActionType = "gc"
	Destroy  ActionType = "destroy"
)

// Object types
const (
	Blob      ObjectType = "blob"
	KVEntry   ObjectType = "kv"
	FS        ObjectType = "fs"
	GitRepo   ObjectType = "git-repo"
	GitNs     ObjectType = "git-ns"
	Namespace ObjectType = "namespace"
)

// Services
const (
	BlobStore ServiceName = "blobstore"
	KvStore   ServiceName = "kvstore"
	DocStore  ServiceName = "docstore"
	Filetree  ServiceName = "filetree"
	GitServer ServiceName = "gitserver"
	Stash     ServiceName = "stash"
)

// Action formats an action `<action_type>:<object_type>`
func Action(action ActionType, objectType ObjectType) string {
	return fmt.Sprintf("action:%s:%s", action, objectType)
}

func ResourceWithID(service ServiceName, objectType ObjectType, objectID string) string {
	return fmt.Sprintf("resource:%s:%s:%s", service, objectType, objectID)
}

func Resource(service ServiceName, objectType ObjectType) string {
	return fmt.Sprintf("resource:%s:%s:NA", service, objectType)
}

func init() {
	SetupRole(&config.Role{
		Name:  "admin",
		Perms: []*config.Perm{&config.Perm{Action: "action:*", Resource: "resource:*"}},
	})
}

var roles = map[string]rbac.Role{}

func SetupRole(r *config.Role) error {
	if _, used := roles[r.Name]; used {
		return fmt.Errorf("%q is already used", r.Name)
	}
	perms := rbac.Permissions{}
	for _, p := range r.Perms {
		if !strings.HasPrefix(p.Action, "action:") {
			return fmt.Errorf("invalid action %q", p.Action)
		}
		if !strings.HasPrefix(p.Resource, "resource:") {
			return fmt.Errorf("invalid resource %q", p.Resource)
		}
		perms = append(perms, rbac.NewGlobPermission(p.Action, p.Resource))
	}

	role := rbac.Role{
		RoleID:      r.Name,
		Permissions: perms,
	}
	roles[r.Name] = role
	return nil
}

func GetRole(k string) (rbac.Role, error) {
	r, ok := roles[k]
	if !ok {
		return rbac.Role{}, fmt.Errorf("role %q not found", k)
	}
	return r, nil
}

func GetRoles(k string) (rbac.Roles, error) {
	res := rbac.Roles{}
	keys := strings.Split(k, ",")
	for _, k := range keys {
		role, err := GetRole(k)
		if err != nil {
			return nil, err
		}
		res = append(res, role)
	}
	return res, nil
}

func Setup(conf *config.Config) error {
	for _, role := range conf.Roles {
		if err := SetupRole(role); err != nil {
			panic(err)
		}
	}
	return nil
}
