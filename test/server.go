package test

import (
	"log"
	"os"
	"io/ioutil"
	"os/exec"
	"path/filepath"
	"bytes"
	"fmt"
	"time"

	"github.com/garyburd/redigo/redis"
)

type TestServer struct {
	binDir string // Path to the bin/ dir of $GOPATH
	rootDir string // The root of the repository
	tempDir string

	server *exec.Cmd // Server process
	buf bytes.Buffer // Store server stdin/stdout

	donec chan struct{} // Notify server shutdown

	err error
}

// NewTestServer initialize a new test server.
func NewTestServer() (*TestServer, error) {
	gopath := os.Getenv("GOPATH")
	if gopath == "" {
		return nil, fmt.Errorf("GOPATH env variable not set")
	}
	binDir := filepath.Join(gopath, "bin")
	rootDir := filepath.Join(gopath, "src/github.com/tsileo/blobstash")
	tempDir, err := ioutil.TempDir("", "blobtools-test-")
	log.Printf("TempDir: %v", tempDir)
	if err != nil {
		return nil, err
	}
	// test/data/blobdb-config.json
	server := &TestServer{binDir: binDir, rootDir: rootDir,
		tempDir: tempDir, donec: make(chan struct{}, 1)}
	return server, nil
}

// Out returns the server Stdout/Stderr
func (server *TestServer) Out() string {
	return server.buf.String()
}

// BuildServer build the "blobdb" binary and return its path
func (server *TestServer) BuildServer() (string, error) {
	blobDbDir := filepath.Join(server.rootDir, "cmd/blobdb")
	cmd := exec.Command("go", "install")
	cmd.Dir = blobDbDir
	log.Print("Running go install to build blobDB...")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("Error installing blobdb: %v, %s", err, string(out))
	}
	log.Print("Done.")
	return filepath.Join(server.binDir, "blobdb"), nil
}

// Ready try to ping the server
func (server *TestServer) Ready() bool {
	c, err := redis.Dial("tcp", ":9735")
	if err != nil {
		return false
	}
	defer c.Close()
	if _, err := c.Do("PING"); err != nil {
		return false
	}
	return true
}

func (server *TestServer) Shutdown() {
	log.Println("Server shutdown")
	c, _ := redis.Dial("tcp", ":9735")
	c.Do("SHUTDOWN")
	server.Wait()
}

// TillReady block until the server is ready
func (server *TestServer) TillReady() error {
	for !server.Ready() {
		if server.err != nil {
			return server.err
		}
		time.Sleep(500 * time.Millisecond)
	}
	log.Println("Server is ready.")
	return nil
}

// Start actuallty start the server
func (server *TestServer) Start() error {
	bpath, err := server.BuildServer()
	if err != nil {
		log.Printf("Error building server")
		server.err = err
		return err
	}
	configPath := filepath.Join(server.rootDir, "test", "data", "blobdb-config.json")
	server.server = exec.Command(bpath, configPath)
	server.server.Stdout = &server.buf
	server.server.Stderr = &server.buf
	server.server.Dir = server.tempDir
	if err := server.server.Start(); err != nil {
		server.err = err
		return err
	}
	waitc := make(chan error, 1)
	go func() {
		waitc <-server.server.Wait()
	}()
	select {
	case serverErr := <-waitc:
		os.RemoveAll("/tmp/blobdb_meta")
		os.RemoveAll("/tmp/blobdb_blobs")
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
