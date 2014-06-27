package glacier

import (
	"testing"

	"github.com/tsileo/blobstash/backend"
)

func TestGlacierBackend(t *testing.T) {
	b := New("blobstashvaultest", "eu-west-1", "tmp_blobsfile_glacier")
	backend.TestWriteOnly(t, b)
}
