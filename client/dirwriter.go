package client

import (
	"io/ioutil"
	"path/filepath"
	"crypto/sha1"
	"fmt"
	"os"
	_ "log"
	"sync"
	"sort"
	"github.com/garyburd/redigo/redis"
)

// DirWriter reads the directory and upload it.
func (client *Client) DirWriterOld(txID, path string) (wr *WriteResult, err error) {
	wg := &sync.WaitGroup{}
	wr = &WriteResult{}
	dirdata, err := ioutil.ReadDir(path)
	if err != nil {
		return
	}
	h := sha1.New()
	hashes := []string{}
	dirdatalen := len(dirdata)
	if dirdatalen != 0 {
		cwrrc := make(chan *WriteResult, dirdatalen)
		errch := make(chan error, dirdatalen)
		for _, data := range dirdata {
			abspath := filepath.Join(path, data.Name())
			if data.IsDir() {
				//wg.Add(1)
				//go func(path string) {
				///	defer wg.Done()
					//_, wr, err := client.PutDir(txID, abspath)
					//if err != nil {
					//	errch <- err
					//} else {
					//	cwrrc <- wr	
					//}
				//}(abspath)
			} else {
				// Skip SymLink for now
				if data.Mode() & os.ModeSymlink == 0 {
					wg.Add(1)
					go func(path string) {
						defer wg.Done()
						_, wr, err := client.PutFile(txID, path)
						if err != nil {
							errch <- err
						} else {
							cwrrc <- wr	
						}
					}(abspath)
				}
			}
		}
		wg.Wait()
		close(errch)
		close(cwrrc)
		for cerr := range errch {
			if cerr != nil {
				return  nil, cerr
			}
		}
		for ccwr := range cwrrc {
			if ccwr.Hash != "" {
				wr.Add(ccwr)
				hashes = append(hashes, ccwr.Hash)	
			}
		}
		// Sort the hashes by lexical order so the hash is deterministic
		sort.Strings(hashes)
		for _, hash := range hashes {
			h.Write([]byte(hash))
		}
		wr.Hash = fmt.Sprintf("%x", h.Sum(nil))
		con := client.Pool.Get()
		defer con.Close()
		if _, err := con.Do("TXINIT", txID); err != nil {
			return wr, err
		}
		// Check if the directory meta already exists 
		cnt, err := redis.Int(con.Do("SCARD", wr.Hash))
		if err != nil {
			return wr, err
		}
		if cnt == 0 {
			if len(hashes) > 0 {
				_, err = con.Do("SADD", redis.Args{}.Add(wr.Hash).AddFlat(hashes)...)
				if err != nil {
					return wr, err
				}
				wr.DirsUploaded++
				wr.DirsCount++
			}
		} else {
			wr.AlreadyExists = true
			wr.DirsSkipped++
			wr.DirsCount++
		}
	} else {
		// If the directory is empty, hash the filename instead of members
		// so an empty directory won't invalidate the directory top hash.
		h.Write([]byte("emptydir:"))
		h.Write([]byte(filepath.Base(path)))
		wr.Hash = fmt.Sprintf("%x", h.Sum(nil))
	}
	//log.Printf("datadb: DirWriter(%v) WriteResult:%+v", path, wr)
	return
}

type node struct {
	root bool
	path string
	fi       os.FileInfo
	children []*node
	wr *WriteResult
	meta *Meta
	err error
	mu sync.Mutex
	cond sync.Cond
}

func (client *Client) DirExplorer(path string, pnode *node, files chan<- *node, result chan<- *node) {
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
		} else {
			if fi.Mode() & os.ModeSymlink == 0 {
				pnode.children = append(pnode.children, n)
				files <- n
			}
		}
	}
	if pnode.root {
		close(files)
		close(result)
	}
	return
}

// DirWriter reads the directory and upload it.
func (client *Client) DirWriterNode(txID string, node *node) {
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
			hashes = append(hashes, cnode.wr.Hash)
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
	node.meta.Hash = node.wr.Hash
	err := node.meta.Save(txID, client.Pool)
	node.err = err
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
	result := make(chan *node)
	fi, _ := os.Stat(abspath)
	n := &node{root: true, path: abspath, fi: fi}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		client.DirExplorer(path, n, files, result)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for f := range files {
			go func(node *node) {
				node.mu.Lock()
				defer node.mu.Unlock()
				meta, wr, err := client.PutFile(txID, node.path)
				node.meta = meta
				node.wr = wr
				node.err = err
				node.cond.Signal()
			}(f)
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for r := range result {
			if !r.root {
				client.DirWriterNode(txID, r)
				r.children = []*node{}
			}
			
		}
	}()

	wg.Wait()
	client.DirWriterNode(txID, n)
	//log.Printf("FINAL NODE:%q", n)
	return n.meta, n.wr, n.err
}

// PutDir upload a directory, it returns the saved Meta,
// a WriteResult containing infos about uploaded blobs.
func (client *Client) PutDirOld(txID, path string) (meta *Meta, wr *WriteResult, err error) {
	//log.Printf("PutDir %v\n", path)
	//abspath, err := filepath.Abs(path)
	//if err != nil {
	//	return
	//}
	//wr, err = client.DirWriter(txID, abspath)
	//if err != nil {
	//	log.Printf("datadb: error DirWriter path:%v/abspath:%v/wr:%+v/err:%v\n", path, abspath, wr, err)
	//	return
	//}
	meta = NewMeta()
	meta.Name = filepath.Base(path)
	meta.Type = "dir"
	meta.Size = wr.Size
	meta.Hash = wr.Hash
	err = meta.Save(txID, client.Pool)
	return
}

