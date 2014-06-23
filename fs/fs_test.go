package fs

import (
	"testing"
	"os"
	"os/exec"
	"io/ioutil"
	"path/filepath"
	"log"

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
	s, err := test.NewTestServer(t)
	check(err)
	go s.Start()
	if err := s.TillReady(); err != nil {
		t.Fatalf("server error:\n%v", err)
	}
	defer s.Shutdown()
	log.Println("Server setup done")
	c, err := client.NewTestClient("")
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
	_, meta, _, err := c.Put(&client.Ctx{Hostname: c.Hostname}, tdir)
	check(err)

	hostname, err := os.Hostname()
	check(err)

	t.Logf("Testing latest directory")

	out, _ := exec.Command("ls", "-lR", tempDir).CombinedOutput()

	restoredPath := filepath.Join(tempDir, hostname, "latest", meta.Name)
	if err := test.Diff(tdir, restoredPath); err != nil {
		t.Logf("ls result: \n%v", string(out))
		t.Errorf("failed to diff the FS: %v\nServer output: %v", err, s.Out())
	}

	stop <-true
	<-stopped
}