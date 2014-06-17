package glacier

import (
	"os"
	"testing"

	"github.com/tsileo/blobstash/backend"
	"github.com/tsileo/blobstash/backend/blobsfile"
)

func TestGlacierBackend(t *testing.T) {
	dest := blobsfile.New("tmp_blobsfile_glacier", 0, false, false)
	defer os.RemoveAll("tmp_blobsfile_glacier")
	b := New(dest)
	backend.TestWriteOnly(t, b)
}
