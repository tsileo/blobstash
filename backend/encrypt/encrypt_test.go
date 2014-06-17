package encrypt

import (
	"os"
	"testing"

	"github.com/tsileo/blobstash/backend"
	"github.com/tsileo/blobstash/backend/blobsfile"
)

func TestEncryptBackend(t *testing.T) {
	dest := blobsfile.New("tmp_blobsfile_enc")
	defer os.RemoveAll("tmp_blobsfile_enc")
	keyPath := "/work/opensource/homedb_gopath/src/github.com/tsileo/blobstash/keytest.key"
	b := New(keyPath, dest)
	backend.Test(t, b)
}
