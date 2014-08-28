package mirror

import (
	"os"
	"testing"

	"github.com/tsileo/blobstash/backend"
	"github.com/tsileo/blobstash/backend/blobsfile"
)

func TestMirrorBackend(t *testing.T) {
	dest1 := blobsfile.New("tmp_blobsfile_1", 0, false, false)
	defer os.RemoveAll("tmp_blobsfile_1")
	dest2 := blobsfile.New("tmp_blobsfile_2", 0, false, false)
	defer os.RemoveAll("tmp_blobsfile_2")
	b := New([]backend.BlobHandler{dest1, dest2}, []backend.BlobHandler{})
	backend.Test(t, b)
}
