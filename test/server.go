package test

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

type TestServer struct {
	t *testing.T

	binDir  string // Path to the bin/ dir of $GOPATH
	rootDir string // The root of the repository
	tempDir string

	server *exec.Cmd    // Server process
	buf    bytes.Buffer // Store server stdin/stdout

	donec chan struct{} // Notify server shutdown

	err error
}

// NewTestServer initialize a new test server.
func NewTestServer(t *testing.T) (*TestServer, error) {
	gopath := os.Getenv("GOPATH")
	if gopath == "" {
		return nil, fmt.Errorf("GOPATH env variable not set")
	}
	binDir := filepath.Join(gopath, "bin")
	rootDir := filepath.Join(gopath, "src/github.com/tsileo/blobstash")
	tempDir, err := ioutil.TempDir("", "blobtools-test-")
	t.Logf("TempDir: %v", tempDir)
	if err != nil {
		return nil, err
	}
	// test/data/blobdb-config.json
	server := &TestServer{t: t, binDir: binDir, rootDir: rootDir,
		tempDir: tempDir, donec: make(chan struct{}, 1)}
	return server, nil
}

// Out returns the server Stdout/Stderr
func (server *TestServer) Out() string {
	return server.buf.String()
}

// BuildServer build the "blobdb" binary and return its path
func (server *TestServer) BuildServer() (string, error) {
	blobDbDir := filepath.Join(server.rootDir, "cmd/blobstash")
	cmd := exec.Command("go", "install")
	cmd.Dir = blobDbDir
	server.t.Log("Running go install to build blobstash...")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("Error installing blobstash: %v, %s", err, string(out))
	}
	server.t.Log("Done")
	return filepath.Join(server.binDir, "blobstash"), nil
}

// Ready try to ping the server
func (server *TestServer) Ready() bool {
	res, err := http.Get("http://127.0.0.1:8050")
	if err == nil {
		res.Body.Close()
		return true
	}
	return false
}

func (server *TestServer) Shutdown() {
	server.server.Process.Kill()
	server.Wait()
}

// TillReady block until the server is ready
func (server *TestServer) TillReady() error {
	for i := 0; i < 100; i++ {
		if server.Ready() {
			server.t.Log("Server is ready.")
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("timeout")
}

// Start actuallty start the server
func (server *TestServer) Start() error {
	bpath, err := server.BuildServer()
	if err != nil {
		server.t.Log("Error building server")
		panic(err)
		server.err = err
		return err
	}
	configPath := filepath.Join(server.rootDir, "test", "data", "blobdb-config.json")
	server.server = exec.Command(bpath, configPath)
	server.server.Stdout = &server.buf
	server.server.Stderr = &server.buf
	server.server.Dir = server.tempDir
	server.server.Env = append(os.Environ(), fmt.Sprintf("BLOBSTASH_VAR_DIR=%v", server.tempDir))
	if err := server.server.Start(); err != nil {
		server.err = err
		panic(err)
		return err
	}
	waitc := make(chan error, 1)
	go func() {
		waitc <- server.server.Wait()
	}()
	select {
	case serverErr := <-waitc:
		os.RemoveAll(server.tempDir)
		server.donec <- struct{}{}
		return fmt.Errorf("server exited: %v", serverErr)
	}
	return nil
}

// Wait till the server exit
func (server *TestServer) Wait() {
	<-server.donec
}
