package fs

import (
	"testing"
	"os"
	"io/ioutil"
	"time"

	"github.com/tsileo/blobstash/test"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func TestFS(t *testing.T) {
	s, err := test.NewTestServer()
	check(err)
	go s.Start()
	s.TillReady()
	defer s.Shutdown()
	tempDir, err := ioutil.TempDir("", "blobtools-blobfs-test-")
	check(err)
	defer os.RemoveAll(tempDir)
	stop := make(chan bool, 1)
	stopped := make(chan bool, 1)
	go Mount(tempDir, stop, stopped)
	// DO TEST HERE
	// random tree with client +
	// test.Diff
	stop <-true
	<-stopped
}