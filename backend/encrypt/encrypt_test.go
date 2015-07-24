package encrypt

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/tsileo/blobstash/backend"
	"github.com/tsileo/blobstash/backend/blobsfile"
)

func TestEncryptBackend(t *testing.T) {
	dest := blobsfile.New("tmp_blobsfile_enc", 0, false, false)
	defer os.RemoveAll("tmp_blobsfile_enc")
	gopath := os.Getenv("GOPATH")
	if gopath == "" {
		panic("GOPATH env variable not set")
	}
	keyPath := filepath.Join(gopath, "src/github.com/tsileo/blobstash/test/data/key.testkey")
	b := New(keyPath, dest)
	backend.Test(t, b)
}
