package client

import (
	"crypto/sha1"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
)

type node struct {
	// root of the snapshot
	root bool

	done bool

	// File path/FileInfo
	path string
	fi   os.FileInfo

	// Children (if the node is a directory)
	children []*node
	parent *node

	rb *ReqBuffer

	// Upload result is stored in the node
	wr   *WriteResult
	meta *Meta
	err  error

	// Used to sync access to the WriteResult/Meta
	mu   sync.Mutex
	cond sync.Cond
}

func (node *node) String() string {
	return fmt.Sprintf("[node %v done=%v, meta=%+v, err=%v]", node.path, node.done, node.meta, node.err)
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
func (client *Client) DirExplorer(path string, pnode *node, nodes chan<- *node) {
	pnode.mu.Lock()
	defer pnode.mu.Unlock()
	dirdata, err := ioutil.ReadDir(path)
	if err != nil {
		return
	}
	for _, fi := range dirdata {
		abspath := filepath.Join(path, fi.Name())
		n := &node{path: abspath, fi: fi, parent: pnode}
		n.cond.L = &n.mu
		if fi.IsDir() {
			n.rb = NewReqBuffer()
			client.DirExplorer(abspath, n, nodes)
			nodes <- n
			pnode.children = append(pnode.children, n)
		} else {
			if fi.Mode()&os.ModeSymlink == 0 {
				if !client.excluded(abspath) {
					nodes <- n
					pnode.children = append(pnode.children, n)
				}
				// else {
				//	log.Printf("DirExplorer: file %v excluded", abspath)
				//}
			}
		}
	}
	pnode.cond.Broadcast()
	return
}

// DirWriter reads the directory and upload it.
func (client *Client) DirWriterNode(ctx *Ctx, node *node) {
	node.mu.Lock()
	defer node.mu.Unlock()
	//log.Printf("DirWriterNode %v star", node)
	node.wr = NewWriteResult()
	h := sha1.New()
	hashes := []string{}

	// Wait for all children node to finish
	for _, cnode := range node.children {
		if cnode.err != nil {
			node.err = cnode.err
			return
		}
		cnode.mu.Lock()
		for !cnode.done {
			cnode.cond.Wait()
		}
		node.wr.Add(cnode.wr)
		cnode.wr.free()
		cnode.wr = nil
		hashes = append(hashes, cnode.meta.Hash)
		cnode.meta.free()
		cnode.meta = nil
		cnode.mu.Unlock()
	}

	client.StartDirUpload()
	defer client.DirUploadDone()

	sort.Strings(hashes)
	for _, hash := range hashes {
		h.Write([]byte(hash))
	}
	node.wr.Hash = fmt.Sprintf("%x", h.Sum(nil))

	con := client.ConnWithCtx(ctx)
	defer con.Close()

	cnt, err := redis.Int(con.Do("SCARD", node.wr.Hash))
	if err != nil {
		node.err = err
		return
	}
	if cnt == 0 {
		if len(hashes) > 0 {
			node.rb.Add("sadd", node.wr.Hash, hashes)
			//_, err = con.Do("SADD", redis.Args{}.Add(node.wr.Hash).Add("").AddFlat(hashes)...)
			//if err != nil {
			//	node.err = err
			//	return
			//}
			node.wr.DirsUploaded++
			node.wr.DirsCount++
		}
	} else {
		node.wr.AlreadyExists = true
		node.wr.DirsSkipped++
		node.wr.DirsCount++
	}

	node.meta = NewMeta()
	node.meta.Name = filepath.Base(node.path)
	node.meta.Type = "dir"
	node.meta.Size = node.wr.Size
	node.meta.Ref = node.wr.Hash
	node.meta.Mode = uint32(node.fi.Mode())
	node.meta.ModTime = node.fi.ModTime().Format(time.RFC3339)
	node.err = node.meta.SaveToBuffer(con, node.rb)
	//_, err = con.Do("TXCOMMIT")
	//if err != nil {
	//	node.err = fmt.Errorf("error TXCOMMIT: %+v", err)
	//	return
	//}

	//if node.rb.ShouldFlush() || node.root {
	//	log.Println("Flushing ReqBuffer now")
	if node.rb.Len() > 1 || node.root {
		_, mblob := node.rb.JSON()
		_, err = con.Do("MBPUT", mblob)
		if err != nil {
			node.err = err
			return
		}
	} else {
		//log.Println("Merging ReqBuffer with parent %v", node.parent)
		node.parent.rb.Merge(node.rb)
	}
	node.done = true
	node.cond.Broadcast()
	return
}

// PutDir upload a directory, it returns the saved Meta,
// a WriteResult containing infos about uploaded blobs.
func (client *Client) PutDir(ctx *Ctx, path string) (*Meta, *WriteResult, error) {
	//log.Printf("PutDir %v\n", path)
	abspath, err := filepath.Abs(path)
	if err != nil {
		return nil, nil, err
	}
	nodes := make(chan *node)
	fi, _ := os.Stat(abspath)
	n := &node{root: true, path: abspath, fi: fi, rb: NewReqBuffer()}
	n.cond.L = &n.mu

	var wg sync.WaitGroup
	// Iterate the directory tree in a goroutine
	// and dispatch node accordingly in the files/result channels.
	wg.Add(1)
	go func() {
		defer wg.Done()
		client.DirExplorer(path, n, nodes)
		defer close(nodes)
		//defer close(directories)
	}()
	// Upload discovered files (100 file descriptor at the same time max).
	wg.Add(1)
	l := make(chan struct{}, 25)
	go func() {
		defer wg.Done()
		for f := range nodes {
			wg.Add(1)
			l <- struct{}{}
			go func(node *node) {
				defer func() {
					<-l
				}()
				defer wg.Done()
				if node.fi.IsDir() {
					client.DirWriterNode(ctx, node)
					if node.err != nil {
						n.err = fmt.Errorf("error DirWriterNode with node %v", node)
					}
				} else {
					node.mu.Lock()
					defer node.mu.Unlock()
					node.meta, node.wr, node.err = client.PutFile(ctx, node.parent.rb, node.path)
					if node.err != nil {
						n.err = fmt.Errorf("error PutFile with node %v", node)
					}
					node.done = true
					node.cond.Broadcast()
				}
			}(f)
		}
	}()
	wg.Wait()
	// Upload the root directory
	client.DirWriterNode(ctx, n)
	con := client.ConnWithCtx(ctx)
	defer con.Close()
	//log.Printf("last node: %v", n)
	return n.meta, n.wr, n.err
}
