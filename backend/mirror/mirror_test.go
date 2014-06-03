package mirror

import (
	"testing"
	"os"

	"github.com/tsileo/datadatabase/backend"
	"github.com/tsileo/datadatabase/backend/blobsfile"
)

func TestMirrorBackend(t *testing.T) {
	dest1 := blobsfile.New("tmp_blobsfile_1", 0, false, false)
	defer os.RemoveAll("tmp_blobsfile_1")
	dest2 := blobsfile.New("tmp_blobsfile_2", 0, false, false)
	defer os.RemoveAll("tmp_blobsfile_2")
	b := New(dest1, dest2)
	backend.Test(t, b)
}
