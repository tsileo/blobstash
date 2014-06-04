package encrypt

import (
	"os"
	"testing"

	"github.com/tsileo/datadatabase/backend"
	"github.com/tsileo/datadatabase/backend/blobsfile"
)

func TestEncryptBackend(t *testing.T) {
	dest := blobsfile.New("tmp_blobsfile_enc")
	defer os.RemoveAll("tmp_blobsfile_enc")
	keyPath := "/work/opensource/homedb_gopath/src/github.com/tsileo/datadatabase/keytest.key"
	b := New(keyPath, dest)
	backend.Test(t, b)
}
