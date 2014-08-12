package clientutil

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/dchest/blake2b"

	"github.com/tsileo/blobstash/client"
	"github.com/tsileo/blobstash/client/transaction"
	"github.com/tsileo/blobstash/client/ctx"
)

// node represents either a file or directory in the directory tree
type node struct {
	// root of the snapshot
	root bool

	done bool

	// File path/FileInfo
	path string
	fi   os.FileInfo

	// Children (if the node is a directory)
	children []*node
	parent   *node

	tx *transaction.Transaction

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
func (up *Uploader) excluded(path string) bool {
	//for _, ignoredFile := range client.ignoredFiles {
	//	matched, _ := filepath.Match(ignoredFile, filepath.Base(path))
	//	if matched {
	//		return true
	//	}
	//}
	return false
}

// Recursively read the directory and
// send/route the files/directories to the according channel for processing
func (up *Uploader) DirExplorer(path string, pnode *node, nodes chan<- *node) {
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
			n.tx = client.NewTransaction()
			up.DirExplorer(abspath, n, nodes)
			nodes <- n
			pnode.children = append(pnode.children, n)
		} else {
			if fi.Mode()&os.ModeSymlink == 0 {
				if !up.excluded(abspath) {
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
func (up *Uploader) DirWriterNode(cctx *ctx.Ctx, node *node) {
	node.mu.Lock()
	defer node.mu.Unlock()
	//log.Printf("DirWriterNode %v star", node)
	node.wr = NewWriteResult()
	h := blake2b.New256()
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

	up.StartDirUpload()
	defer up.DirUploadDone()

	sort.Strings(hashes)
	for _, hash := range hashes {
		h.Write([]byte(hash))
	}
	node.wr.Hash = fmt.Sprintf("%x", h.Sum(nil))

	con := up.client.ConnWithCtx(cctx)
	defer con.Close()

	cnt, err := up.client.Scard(con, node.wr.Hash)
	if err != nil {
		node.err = err
		return
	}
	if cnt == 0 {
		if len(hashes) > 0 {
			node.tx.Sadd(node.wr.Hash, hashes...)
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
	node.meta.ComputeHash()
	hlen, err := up.client.Hlen(con, node.meta.Hash)
	if err != nil {
		node.err = err
		return
	}
	if hlen == 0 {
		node.tx.Hmset(node.meta.Hash, client.FormatStruct(node.meta)...)
	}
	//if node.rb.ShouldFlush() || node.root {
	//	log.Println("Flushing ReqBuffer now")
	if node.tx.Len() > 3 || node.root {
		if err := up.client.Commit(cctx, node.tx); err != nil {
			node.err = err
			return
		}
	} else {
		//log.Println("Merging ReqBuffer with parent %v", node.parent)
		node.parent.tx.Merge(node.tx)
	}
	node.done = true
	node.cond.Broadcast()
	return
}

// PutDir upload a directory, it returns the saved Meta,
// a WriteResult containing infos about uploaded blobs.
func (up *Uploader) PutDir(cctx *ctx.Ctx, path string) (*Meta, *WriteResult, error) {
	//log.Printf("PutDir %v\n", path)
	abspath, err := filepath.Abs(path)
	if err != nil {
		return nil, nil, err
	}
	nodes := make(chan *node)
	fi, _ := os.Stat(abspath)
	n := &node{root: true, path: abspath, fi: fi, tx: client.NewTransaction()}
	n.cond.L = &n.mu

	var wg sync.WaitGroup
	// Iterate the directory tree in a goroutine
	// and dispatch node accordingly in the files/result channels.
	wg.Add(1)
	go func() {
		defer wg.Done()
		up.DirExplorer(path, n, nodes)
		defer close(nodes)
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
					up.DirWriterNode(cctx, node)
					if node.err != nil {
						n.err = fmt.Errorf("error DirWriterNode with node %v", node)
					}
				} else {
					node.mu.Lock()
					defer node.mu.Unlock()
					node.meta, node.wr, node.err = up.PutFile(cctx, node.parent.tx, node.path)
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
	up.DirWriterNode(cctx, n)
	return n.meta, n.wr, n.err
}
