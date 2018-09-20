package perms

import (
	"testing"
)

func TestPerms(t *testing.T) {
	res, err := Admin.Can(Action(Write, Blob), Resource(BlobStore, Blob, "deadbeef"))
	if err != nil {
		panic(err)
	}
	if !res {
		t.Errorf("admin should be allowed to do that")
	}
}
