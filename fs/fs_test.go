package fs

import (
	"testing"
	"os"
	"io/ioutil"
	"path/filepath"
	"time"

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

	// Setup client
	c, err := client.NewTestClient()
	defer c.Close()
	defer c.RemoveCache()
	check(err)
	tdir := test.NewRandomTree(t, ".", 1)
	defer os.RemoveAll(tdir)
	meta, _, err := c.PutDir(tdir)
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
	time.Sleep(30*time.Second)

	restoredPath := filepath.Join(tempDir, "latest", meta.Name)
	check(test.Diff(tdir, restoredPath))

	stop <-true
	<-stopped
}