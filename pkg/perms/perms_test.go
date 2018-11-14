package perms

import (
	"testing"

	"a4.io/blobstash/pkg/config"
)

func setupTestRole(name, action, resource string) *config.Role {
	return &config.Role{
		Name:  name,
		Perms: []*config.Perm{&config.Perm{action, resource}},
	}
}

func TestPerms(t *testing.T) {
	if err := SetupRole(setupTestRole("admin3", "action:*", "resource:*")); err != nil {
		panic(err)
	}

	admin, err := GetRole("admin3")
	if err != nil {
		panic(err)
	}
	res, err := admin.Can(Action(Write, Blob), ResourceWithID(BlobStore, Blob, "deadbeef"))
	if err != nil {
		panic(err)
	}
	if !res {
		t.Errorf("admin should be allowed to do that")
	}
}

func TestPermsRoles(t *testing.T) {
	if err := SetupRole(setupTestRole("admin2", "action:*", "resource:*")); err != nil {
		panic(err)
	}

	admin, err := GetRoles([]string{"admin", "admin2"})
	if err != nil {
		panic(err)
	}
	res, err := admin.Can(Action(Write, Blob), ResourceWithID(BlobStore, Blob, "deadbeef"))
	if err != nil {
		panic(err)
	}
	if !res {
		t.Errorf("admin should be allowed to do that")
	}
}

func TestRedefineAdmin(t *testing.T) {
	if err := SetupRole(setupTestRole("admin", "action:read:blob", "resource:*")); err == nil {
		t.Errorf("err should not be nil, got %v", err)
	}
}
