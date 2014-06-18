package fs

import (
	"testing"
	"os"
	"os/exec"
	"io/ioutil"
	"path/filepath"

	"github.com/tsileo/blobstash/test"
	"github.com/tsileo/blobstash/client"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func TestFS(t *testing.T) {
	// Setup server
	s, err := test.NewTestServer()
	check(err)
	go s.Start()
	s.TillReady()
	defer s.Shutdown()

	c, err := client.NewTestClient()
	defer c.Close()
	defer c.RemoveCache()
	check(err)

	// Setup FS
	tempDir, err := ioutil.TempDir("", "blobtools-blobfs-test-")
	check(err)
	defer os.RemoveAll(tempDir)
	stop := make(chan bool, 1)
	stopped := make(chan bool, 1)
	go Mount(tempDir, stop, stopped)
	// DO TEST HERE
	// random tree with client +
	// test.Diff

	tdir := test.NewRandomTree(t, ".", 1)
	defer os.RemoveAll(tdir)
	meta, _, err := c.PutDir(tdir)
	check(err)

	t.Logf("DIFF")

	out, err := exec.Command("ls", "-lR", tempDir).CombinedOutput()

	t.Logf("ls result (%v): \n%v", err, string(out))

	restoredPath := filepath.Join(tempDir, "latest", meta.Name)
	if err := test.Diff(tdir, restoredPath); err != nil {
		t.Errorf("failed to diff the FS: %v\nServer output: %v", err, s.Out())
	}

	stop <-true
	<-stopped
}