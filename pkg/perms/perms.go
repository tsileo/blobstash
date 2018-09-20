package perms // import "a4.io/blobstash/pkg/perms"

import (
	"fmt"

	"github.com/zpatrick/rbac"
)

type ActionType string
type ObjectType string
type ServiceName string

// Actions
const (
	Read  ActionType = "read"
	Write ActionType = "write"
)

// Object types
const (
	Blob ObjectType = "blob"
)

// Services
const (
	BlobStore ServiceName = "blobstore"
	KvStore   ServiceName = "kvstore"
	DocStore  ServiceName = "docstore"
	Filetree  ServiceName = "filetree"
	GitServer ServiceName = "gitserver"
)

// Action formats an action `<action_type>:<object_type>`
func Action(action ActionType, objectType ObjectType) string {
	return fmt.Sprintf("action:%s:%s", action, objectType)
}

func Resource(service ServiceName, objectType ObjectType, objectID string) string {
	return fmt.Sprintf("resource:%s:%s:%s", service, objectType, objectID)
}

var (
	Admin = rbac.Role{
		RoleID: "admin",
		Permissions: []rbac.Permission{
			rbac.NewGlobPermission("action:*", "resource:*"),
		},
	}
)
