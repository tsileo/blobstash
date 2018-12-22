package writer

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"sync"

	rnode "a4.io/blobstash/pkg/filetree/filetreeutil/node"
)

// node represents either a file or directory in the directory tree
type node struct {
	// root of the snapshot
	root    bool
	skipped bool

	done bool

	// File path/FileInfo
	path string
	fi   os.FileInfo

	// Children (if the node is a directory)
	children []*node
	parent   *node

	// Upload result is stored in the node
	// wr   *WriteResult
	meta *rnode.RawNode
	err  error

	// Used to sync access to the WriteResult/Meta
	mu   sync.Mutex
	cond sync.Cond
}

func (node *node) String() string {
	return fmt.Sprintf("[node %v done=%v, meta=%+v, err=%v]", node.path, node.done, node.meta, node.err)
}

// DirExplorer recursively reads the directory and
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
		// relpath, err := filepath.Rel(up.Root, abspath)
		// if err != nil {
		// 	panic(err)
		// }
		// if up.Ignorer != nil && up.Ignorer.MatchesPath(relpath) {
		// 	log.Printf("Uploader: %v excluded", relpath)
		// 	continue
		// }
		n := &node{path: abspath, fi: fi, parent: pnode}
		n.cond.L = &n.mu
		if fi.IsDir() {
			up.DirExplorer(abspath, n, nodes)
			nodes <- n
			pnode.children = append(pnode.children, n)
		} else {
			if fi.Mode()&os.ModeSymlink == 0 {
				nodes <- n
				pnode.children = append(pnode.children, n)
			}
		}
	}
	pnode.cond.Broadcast()
	return
}

// DirWriterNode reads the directory and upload it.
func (up *Uploader) DirWriterNode(node *node) {
	node.mu.Lock()
	defer node.mu.Unlock()

	ctx := context.TODO()

	// node.wr = NewWriteResult()
	hashes := []string{}

	// Wait for all children node to finish
	node.skipped = true
	for _, cnode := range node.children {
		cnode.mu.Lock()
		for !cnode.done {
			cnode.cond.Wait()
		}
		if cnode.err != nil {
			if !os.IsPermission(cnode.err) {
				return
			}
		}
		node.skipped = node.skipped && cnode.skipped
		// node.wr.Add(cnode.wr)
		// cnode.wr.free()
		// cnode.wr = nil

		// If a permission error prevented the uploader from reading the file, just ignore this node
		// (but only for children of dir)
		// TODO(tsileo): report it somewhere?
		if !os.IsPermission(cnode.err) {
			hashes = append(hashes, cnode.meta.Hash)
		}
		cnode.meta = nil
		cnode.mu.Unlock()
	}
	up.StartDirUpload()
	defer up.DirUploadDone()

	node.meta = &rnode.RawNode{
		Version: rnode.V1,
	}
	sort.Strings(hashes)
	for _, hash := range hashes {
		node.meta.AddRef(hash)
	}
	// if node.skipped {
	// 	node.wr.DirsSkipped++
	// } else {
	// 	node.wr.DirsUploaded++
	// }
	// node.wr.DirsCount++
	// TODO WriteResult exisiting handling
	node.meta.Name = filepath.Base(node.path)
	node.meta.Type = "dir"
	// node.meta.Size = node.wr.Size
	mhash, mjs := node.meta.Encode()
	node.meta.Hash = mhash
	mexists, err := up.bs.Stat(ctx, mhash)
	if err != nil {
		node.err = err
		return
	}
	if !mexists {
		if err := up.bs.Put(ctx, mhash, mjs); err != nil {
			node.err = err
			return
		}
		// node.wr.BlobsCount++
		// node.wr.BlobsUploaded++
		// node.wr.SizeUploaded += len(mjs)
	} // else {
	// node.wr.SizeSkipped += len(mjs)
	// }
	node.done = true
	node.cond.Broadcast()
	return
}

// PutDir upload a directory, it returns the saved Meta,
// a WriteResult containing infos about uploaded blobs.
func (up *Uploader) PutDir(path string) (*rnode.RawNode, error) {
	//log.Printf("PutDir %v\n", path)
	abspath, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	up.Root = path
	// if _, err := os.Stat(filepath.Join(path, ".blobsnapignore")); err == nil {
	// 	ignorer, err := gignore.CompileIgnoreFile(filepath.Join(path, ".blobsnapignore"))
	// 	if err != nil {
	// 		return nil, nil, fmt.Errorf("failed to parse .blobsnapignore file: %v", err)
	// 	}
	// 	up.Ignorer = ignorer
	// }
	nodes := make(chan *node)
	fi, _ := os.Stat(abspath)
	n := &node{root: true, path: abspath, fi: fi}
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
	// Upload discovered files (5 file descriptor at the same time max).
	wg.Add(1)
	l := make(chan struct{}, 5)
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
					up.DirWriterNode(node)
					if node.err != nil {
						n.err = fmt.Errorf("error DirWriterNode with node %v", node)
					}
				} else {
					node.mu.Lock()
					defer node.mu.Unlock()
					node.meta, node.err = up.PutFile(node.path)
					if node.err != nil {
						if !os.IsPermission(node.err) {
							n.err = fmt.Errorf("error PutFile with node %v", node)
						}
					}
					// if node.wr.FilesSkipped == 1 {
					// 	node.skipped = true
					// }
					node.done = true
					node.cond.Broadcast()
				}
			}(f)
		}
	}()
	wg.Wait()
	// Upload the root directory
	up.DirWriterNode(n)
	return n.meta, n.err
}
