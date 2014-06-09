package glacier

import (
	"os"
	"testing"

	"github.com/tsileo/datadatabase/backend"
	"github.com/tsileo/datadatabase/backend/blobsfile"
)

func TestGlacierBackend(t *testing.T) {
	dest := blobsfile.New("tmp_blobsfile_glacier", 0, false, false)
	defer os.RemoveAll("tmp_blobsfile_glacier")
	b := New(dest)
	backend.TestWriteOnly(t, b)
}
