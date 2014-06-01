package client

import (
	"io/ioutil"
	"path/filepath"
	"crypto/sha1"
	"fmt"
	"os"
	"log"
	"sync"
	"sort"
	"github.com/garyburd/redigo/redis"
)

type node struct {
	// root of the snapshot
	root bool

	// File path/FileInfo
	path string
	fi       os.FileInfo

	// Children (if the node is a directory)
	children []*node

	// Upload result is stored in the node
	wr *WriteResult
	meta *Meta
	err error

	// Used to sync access to the WriteResult/Meta
	mu sync.Mutex
	cond sync.Cond
}

// excluded returns true if the base path match one of the defined shell pattern
func (client *Client) excluded(path string) bool {
	for _, ignoredFile := range client.ignoredFiles {
		matched, _ := filepath.Match(ignoredFile, filepath.Base(path))
		if matched {
			return true
		}
	}
	return false
}

// Recursively read the directory and
// send/route the files/directories to the according channel for processing
func (client *Client) DirExplorer(path string, pnode *node, files chan<- *node, result chan<- *node) {
	pnode.mu.Lock()
	defer pnode.mu.Unlock()
	dirdata, err := ioutil.ReadDir(path)
	if err != nil {
		return
	}
	for _, fi := range dirdata {
		abspath := filepath.Join(path, fi.Name())
		n := &node{path:abspath, fi: fi}
		n.cond.L = &n.mu
		if fi.IsDir() {
			client.DirExplorer(abspath, n, files, result)
			result <- n
			pnode.children = append(pnode.children, n)
		} else {
			if fi.Mode() & os.ModeSymlink == 0 {
				if client.excluded(abspath) {
					log.Printf("DirExplorer: file %v excluded", abspath)
				} else {
					files <- n
					pnode.children = append(pnode.children, n)
				}
			}
		}
	}
	pnode.cond.Signal()
	return
}

// DirWriter reads the directory and upload it.
func (client *Client) DirWriterNode(txID string, node *node) {
	node.mu.Lock()
	defer node.mu.Unlock()
	node.wr = &WriteResult{}
	h := sha1.New()
	hashes := []string{}
	dirdatalen := len(node.children)
	if dirdatalen != 0 {
		for _, cnode := range node.children {
			cnode.mu.Lock()
			if cnode.wr == nil && cnode.err == nil {
				cnode.cond.Wait()
			}
			node.wr.Add(cnode.wr)
			if cnode.meta.Hash == "" {
				panic(fmt.Errorf("bad cnode in DirWriterNode %q: %q", node, cnode))
			}
			hashes = append(hashes, cnode.meta.Hash)
			cnode.mu.Unlock()
		}
		// Sort the hashes by lexical order so the hash is deterministic
		sort.Strings(hashes)
		for _, hash := range hashes {
			h.Write([]byte(hash))
		}
		node.wr.Hash = fmt.Sprintf("%x", h.Sum(nil))
		con := client.Pool.Get()
		defer con.Close()
		if _, err := con.Do("TXINIT", txID); err != nil {
			node.err = err
			return
		}
		// Check if the directory meta already exists
		cnt, err := redis.Int(con.Do("SCARD", node.wr.Hash))
		if err != nil {
			node.err = err
			return
		}
		if cnt == 0 {
			if len(hashes) > 0 {
				_, err = con.Do("SADD", redis.Args{}.Add(node.wr.Hash).AddFlat(hashes)...)
				if err != nil {
					node.err = err
					return
				}
				node.wr.DirsUploaded++
				node.wr.DirsCount++
			}
		} else {
			node.wr.AlreadyExists = true
			node.wr.DirsSkipped++
			node.wr.DirsCount++
		}
	} else {
		// If the directory is empty, hash the filename instead of members
		// so an empty directory won't invalidate the directory top hash.
		h.Write([]byte("emptydir:"))
		h.Write([]byte(filepath.Base(node.path)))
		node.wr.Hash = fmt.Sprintf("%x", h.Sum(nil))
	}
	node.meta = NewMeta()
	node.meta.Name = filepath.Base(node.path)
	node.meta.Type = "dir"
	node.meta.Size = node.wr.Size
	node.meta.Ref = node.wr.Hash
	err := node.meta.Save(txID, client.Pool)
	node.err = err
	node.cond.Signal()
	return
}

// PutDir upload a directory, it returns the saved Meta,
// a WriteResult containing infos about uploaded blobs.
func (client *Client) PutDir(txID, path string) (meta *Meta, wr *WriteResult, err error) {
	//log.Printf("PutDir %v\n", path)
	abspath, err := filepath.Abs(path)
	if err != nil {
		return
	}
	files := make(chan *node)
	directories := make(chan *node)
	dirSem := make(chan struct{}, 50)
	fi, _ := os.Stat(abspath)
	n := &node{root: true, path: abspath, fi: fi}
	n.cond.L = &n.mu

	var wg sync.WaitGroup
	// Iterate the directory tree in a goroutine
	// and dispatch node accordingly in the files/result channels.
	wg.Add(1)
	go func() {
		defer wg.Done()
		client.DirExplorer(path, n, files, directories)
		defer close(files)
		defer close(directories)
	}()
	// Upload discovered files (100 file descriptor at the same time max).
	wg.Add(1)
	go func() {
		defer wg.Done()
		for f := range files {
			go func(node *node) {
				node.mu.Lock()
				defer node.mu.Unlock()
				node.meta, node.wr, node.err = client.PutFile(txID, node.path)
				if node.err != nil {
					panic(fmt.Errorf("Error PutFile with node %q", node))
				}
				node.cond.Signal()
			}(f)
		}
	}()
	// Save directories meta data
	wg.Add(1)
	go func() {
		defer wg.Done()
		for d := range directories {
			//log.Printf("waiting to aquire dir:%q", d)
			dirSem <- struct{}{}
			go func(node *node) {
				defer func() {
					<-dirSem
				}()
				client.DirWriterNode(txID, node)
				if node.err != nil {
					panic(fmt.Errorf("Error DirWriterNode with node %q", node))	
				}
				// TODO(tsileo) check that r.wr is up to date.
			}(d)
		}
	}()

	wg.Wait()
	// Upload the root directory
	log.Printf("last node")
	client.DirWriterNode(txID, n)
	return n.meta, n.wr, n.err
}
