package main

// import "a4.io/blobstash/pkg/filetree/fs"

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"mime/multipart"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/dchest/blake2b"
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
	"github.com/hashicorp/golang-lru"
	"github.com/pkg/xattr"

	bcache "a4.io/blobstash/pkg/cache"
	"a4.io/blobstash/pkg/client/clientutil"
	"a4.io/blobstash/pkg/client/kvstore"
	"a4.io/blobstash/pkg/client/oplog"
	rnode "a4.io/blobstash/pkg/filetree/filetreeutil/node"
	"a4.io/blobstash/pkg/filetree/reader/filereader"
)

// TODO(tsileo):
// - [X] default to RO mode
// - support file@<date> ; e.g.: file.txt@2017-5-4T21:30 ???
// - [/] `-snapshot` mode that lock to the current version, very efficient, can specify a snapshot version `-at`
// - [X] `-rw` mode that support mutation, will query BlobStah a lot (without `-live-update`)
// - [X] `-live-update` (or `-ro`?) mode to receive update via SSE (e.g. serve static over fuse, or Dropbox infinite like), allow caching and to invalidate cache on remote changes (need to be able to discard its own generated event via the hostname)
// - [ ] support data context (add timeout server-side), and merge the data context on unmount
// - [ ] Fake the file that disable MacOS Finder to crawl it

var kvs *kvstore.KvStore
var cache *Cache
var owner *fuse.Owner
var mu sync.Mutex

type FSUpdateEvent struct {
	Name      string `json:"fs_name"`
	Path      string `json:"fs_path"`
	Ref       string `json:"node_ref"`
	Type      string `json:"node_type"`
	Time      int64  `json:"event_time"`
	Hostname  string `json:"event_hostname"`
	SessionID string `json:"session_id"`
}

func EventFromJSON(data string) *FSUpdateEvent {
	out := &FSUpdateEvent{}
	if err := json.Unmarshal([]byte(data), out); err != nil {
		panic(err)
	}
	return out
}

func main() {
	// Scans the arg list and sets up flags
	debug := flag.Bool("debug", false, "print debugging messages.")
	rw := flag.Bool("rw", false, "read-write mode.")
	liveUpdate := flag.Bool("live-update", false, "receive FS notifications from BlobStash for efficient caching.")
	snapshot := flag.Bool("snapshot", false, "lock the FS version.")
	// TODO(tsileo): support ref in getNode
	ref := flag.String("ref", "", "mount the given node (via its hex-encoded ref) as root, will default to snapshot mode.")
	// FIXME(tsileo): asOf flag for snapshot

	if *ref != "" {
		if *liveUpdate {
			fmt.Printf("Cannot enable live update when mounting a ref")
			os.Exit(1)
		}
		if *rw {
			fmt.Printf("Cannot enable read-write mode when mounting a ref")
			os.Exit(1)
		}
		*snapshot = true
	}

	flag.Parse()
	if flag.NArg() < 2 {
		fmt.Fprintf(os.Stderr, "usage: %s MOUNTPOINT REF\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(2)
	}

	var err error
	cache, err = newCache("fs_cache", *liveUpdate, *snapshot)
	if err != nil {
		fmt.Printf("failed to setup cache: %v\n", err)
		os.Exit(1)
	}
	kvopts := kvstore.DefaultOpts().SetHost(os.Getenv("BLOBS_API_HOST"), os.Getenv("BLOBS_API_KEY"))
	kvopts.SnappyCompression = false
	kvs = kvstore.New(kvopts)

	oplogClient := oplog.New(kvopts)
	ops := make(chan *oplog.Op)
	root := NewFileSystem(flag.Arg(1), *debug, *rw, *snapshot, *liveUpdate)

	opts := &nodefs.Options{
		Debug: false, // *debug,
	}
	nfs := pathfs.NewPathNodeFs(root, nil)
	// state, _, err := nodefs.MountRoot(flag.Arg(0), nfs.Root(), opts)

	conn := nodefs.NewFileSystemConnector(nfs.Root(), opts)

	// XXX(tsileo): different options on READ ONLY mode
	mountOpts := fuse.MountOptions{
		Options: []string{
			// FIXME(tsileo): no more nolocalcaches and use notify instead for linux
			"allow_root",
			"allow_other",
			"nolocalcaches",
			"defer_permissions",
			"noclock",
			"auto_xattr",
			"noappledouble",
			"noapplexattr",
			"volname=BlobFS." + flag.Arg(1),
		},
		FsName: "blobfs",
	}
	if opts != nil && opts.Debug {
		mountOpts.Debug = opts.Debug
	}
	state, err := fuse.NewServer(conn.RawFS(), flag.Arg(0), &mountOpts)
	if err != nil {
		fmt.Printf("failed to init FUSE server: %v\n", err)
		os.Exit(1)
	}

	if err != nil {
		fmt.Printf("Mount fail: %v\n", err)
		os.Exit(1)
	}

	owner = fuse.CurrentOwner()

	if *liveUpdate {
		go func() {
			var errsCount int
			for {
				if err := oplogClient.Notify(ops, func() {
					// FIXME(tsileo): re-enable the cache
					cache.mu.Lock()
					cache.reconnectPending = false
					cache.mu.Unlock()
					log.Println("Oplog connection succeed")
					errsCount = 0
				}); err != nil {
					cache.mu.Lock()
					cache.reconnectPending = true
					cache.mu.Unlock()
					log.Printf("Oplog connection lost: %v", err)
					log.Println("Oplog reconnection...")
					errsCount++
				}
				if *debug {
					log.Println("Resetting cache...")
				}

				cache.mu.Lock()
				cache.nodeIndex = map[string]*Node{}
				cache.negNodeIndex = map[string]struct{}{}
				// TODO(tsileo): add Pending() mode so nothing is cached while we reconnect
				// in other words: disable the cache while we're not connected (maybe add a optional callback func to notify?)
				cache.mu.Unlock()
				// look at replication backoff for BlobStash
				time.Sleep(1 * time.Second * time.Duration(errsCount))
			}
		}()
		go func() {
			for op := range ops {
				if op.Event == "filetree" {
					fmt.Printf("op=%+v\n", op)
					evt := EventFromJSON(op.Data)
					fmt.Printf("evt=%+v\n", evt)
					if evt.SessionID == kvs.Client().SessionID() {
						// If this host generated the event, we just discard it
						continue
					}
					// switch evt.Type {
					// case "file-updated":
					if err := nfs.Notify(evt.Path); err != fuse.OK {
						fmt.Printf("failed to notify=%+v\n", err)
					}
					func() {
						cache.mu.Lock()
						defer cache.mu.Unlock()
						if _, ok := cache.nodeIndex[evt.Path]; ok {
							delete(cache.nodeIndex, evt.Path)
						}
						if _, ok := cache.negNodeIndex[evt.Path]; ok {
							delete(cache.negNodeIndex, evt.Path)
						}
						// Remove the cache up to the root
						p := filepath.Dir(evt.Path)
						for {
							if p == "." {
								p = ""
							}
							if _, ok := cache.nodeIndex[p]; ok {
								delete(cache.nodeIndex, p)
							}
							if _, ok := cache.negNodeIndex[p]; ok {
								delete(cache.negNodeIndex, p)
							}
							if p == "" {
								break
							}
							p = filepath.Dir(p)
						}
					}()
				}
			}
		}()
	}
	go state.Serve()
	log.Printf("mounted successfully")

	// Be ready to cleanup if we receive a kill signal
	cs := make(chan os.Signal, 1)
	signal.Notify(cs, os.Interrupt,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	<-cs
	if err := state.Unmount(); err != nil {
		fmt.Printf("failed to unmount: %s", err)
		os.Exit(1)
	}
	os.Exit(0)
}

type Node struct {
	Name     string                 `json:"name"`
	Ref      string                 `json:"ref"`
	Size     int                    `json:"size"`
	Type     string                 `json:"type"`
	Children []*Node                `json:"children"`
	Metadata map[string]interface{} `json:"metadata"`
	ModTime  string                 `json:"mtime"`
}

func (n *Node) IsDir() bool {
	return n.Type == "dir"
}

func (n *Node) IsFile() bool {
	return n.Type == "file"
}

func (n *Node) Mtime() uint64 {
	if n.ModTime != "" {
		t, err := time.Parse(time.RFC3339, n.ModTime)
		if err != nil {
			panic(err)
		}
		return uint64(t.Unix())
	}
	return 0
}

func (n *Node) Copy(dst io.Writer) error {
	resp, err := kvs.Client().DoReq("GET", "/api/filetree/file/"+n.Ref, nil, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	fmt.Printf("resp=%+v\n", resp)
	n2, err := io.Copy(dst, resp.Body)
	fmt.Printf("copied %s bytes (Node.Copy)\n", n2)
	if err != nil {
		return err
	}
	return nil
}

func (n *Node) ReadAt(buf []byte, off int64) ([]byte, fuse.Status) {
	end := int(off) + int(len(buf))
	if end > n.Size {
		end = n.Size
	}
	rangeVal := fmt.Sprintf("bytes=%d-%d", off, end-1)
	resp, err := kvs.Client().DoReq("GET", "/api/filetree/file/"+n.Ref, map[string]string{"Range": rangeVal}, nil)
	if err != nil {
		return nil, fuse.EIO
	}
	defer resp.Body.Close()
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fuse.EIO
	}
	// fmt.Printf("ReadAt %d %d %s %d\n", len(buf), off, rangeVal, len(data))
	if resp.StatusCode != 206 {
		fmt.Printf("request failed: url=%s, status=%d, range=%s, error=%s\n", "/api/filetree/file/"+n.Ref, resp.StatusCode, rangeVal, data)
		return nil, fuse.EIO
	}
	return data, fuse.OK
}

type loopbackFile struct {
	nodefs.File
	node *WritableNode
	lock sync.Mutex
}

func (f *loopbackFile) Release() {
	fmt.Printf("file release")
	// FIXME(tsileo): check release behavior (on file close or last fd closed?)
	cache.mu.Lock()
	defer cache.mu.Unlock()
	f.node.f.Close()
	openedFile := cache.openedFiles[f.node.path]
	openedFile.fds--
	if openedFile.fds == 0 {
		fmt.Printf("last opened file, removing tmp")
		if err := os.Remove(openedFile.tmpPath); err != nil {
			panic(err)
		}
		delete(cache.openedFiles, f.node.path)
	}
}

func (f *loopbackFile) Flush() fuse.Status {
	mu.Lock()
	defer mu.Unlock()
	if status := f.File.Flush(); status != fuse.OK {
		return status
	}
	fmt.Printf("CUSTOM FLUSH")
	// FIXME(tsileo): ensure the file has been modified!
	// TODO(tsileo): in the future, chunk **big** files locally to prevent sending everyting (don't forget
	// the SessionID)

	bodyBuf := &bytes.Buffer{}
	bodyWriter := multipart.NewWriter(bodyBuf)
	fileWriter, err := bodyWriter.CreateFormFile("file", "file")
	if err != nil {
		fmt.Printf("form file failed\n")
		return fuse.EIO
	}
	contentType := bodyWriter.FormDataContentType()

	fi, err := os.Open(f.node.tmpPath)
	if err != nil {
		fmt.Printf("open tmp failed")
		return fuse.EIO
	}
	defer fi.Close()
	if _, err := io.Copy(fileWriter, fi); err != nil {
		fmt.Printf("copy failed:%v\n", err)
		return fuse.EIO
	}
	bodyWriter.Close()

	resp, err := kvs.Client().DoReq("POST", "/api/filetree/fs/fs/"+f.node.ref+"/"+f.node.path, map[string]string{
		"Content-Type": contentType,
	}, bodyBuf)
	fmt.Printf("FLUSH resp=%+v, err=%+v\n", resp, err)
	if err != nil || resp.StatusCode != 200 {
		return fuse.EIO
	}
	defer resp.Body.Close()
	node := &Node{}
	if err := json.NewDecoder(resp.Body).Decode(node); err != nil {
		return fuse.EIO
	}
	cache.mu.Lock()
	cache.nodeIndex[f.node.path] = node
	cache.mu.Unlock()
	fmt.Printf("CUSTOM FLUSH OK")

	return fuse.OK
}

func newLoopbackFile(f nodefs.File, n *WritableNode) nodefs.File {
	return &loopbackFile{
		File: f,
		node: n,
	}
}

type WritableNode struct {
	node    *Node
	path    string
	tmpPath string
	f       *os.File
	ref     string
}

type openedFile struct {
	path    string
	tmpPath string
	ref     string
	fds     int
}

type Cache struct {
	openedFiles      map[string]*openedFile
	nodeIndex        map[string]*Node
	negNodeIndex     map[string]struct{}
	mu               sync.Mutex
	path             string
	blobsCache       *bcache.Cache
	liveUpdate       bool
	snapshot         bool
	reconnectPending bool
}

func newCache(path string, liveUpdate, snapshot bool) (*Cache, error) {
	if err := os.RemoveAll(path); err != nil {
		return nil, err
	}
	if err := os.Mkdir(path, 0700); err != nil {
		return nil, err
	}
	blobsCache, err := bcache.New(".", "blobs.cache", 256<<20) // 256MB on-disk LRU cache
	if err != nil {
		return nil, err
	}

	return &Cache{
		openedFiles:      map[string]*openedFile{},
		nodeIndex:        map[string]*Node{},
		negNodeIndex:     map[string]struct{}{},
		path:             path,
		blobsCache:       blobsCache,
		liveUpdate:       liveUpdate,
		snapshot:         snapshot,
		reconnectPending: liveUpdate, // Wait for oplog connection before enabling the cache if live updates are enabled
	}, nil
}

// Get implements the BlobStore interface for filereader.File
func (c *Cache) Get(ctx context.Context, hash string) ([]byte, error) {
	cachedBlob, ok, err := c.blobsCache.Get(hash)
	if err != nil {
		return nil, err
	}
	var data []byte
	if ok {
		data = cachedBlob
	} else {
		resp, err := kvs.Client().DoReq("GET", "/api/blobstore/blob/"+hash, nil, nil)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		data, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		if err := c.blobsCache.Add(hash, data); err != nil {
			return nil, err
		}
	}
	return data, nil
}

func (c *Cache) newWritableNode(ref, path string, mode uint32) (nodefs.File, error) {
	// XXX(tsileo): use hash for consistent filename, only load the node into the file if it just got created
	// or return a fd to the already present file
	// FIXME(tsileo): use a custom cache dir
	var err error
	var shouldLoad bool
	var fpath string
	var tmpFile *os.File
	cdata, alreadyOpen := cache.openedFiles[path]
	if alreadyOpen {
		tmpFile, err = os.OpenFile(cdata.tmpPath, os.O_RDWR, os.FileMode(mode))
		if err != nil {
			return nil, err
		}
	} else {
		tmpFile, err = ioutil.TempFile("", "blobfs_node")
		if err != nil {
			return nil, err
		}
		fpath = tmpFile.Name()
		shouldLoad = true
	}
	var n *Node

	// Copy the original content if the node already exists
	n, err = c.getNode(ref, path)
	switch err {
	case nil:
		if !shouldLoad {
			break
		}

		tmtime := time.Unix(int64(n.Mtime()), 0)
		if err := os.Chtimes(fpath, tmtime, tmtime); err != nil {
			return nil, err
		}
		if err := n.Copy(tmpFile); err != nil {
			return nil, err
		}
		if _, err := tmpFile.Seek(0, os.SEEK_SET); err != nil {
			return nil, err
		}

		if err := tmpFile.Sync(); err != nil {
			return nil, err
		}
	case clientutil.ErrNotFound:
	default:
		return nil, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	wnode := &WritableNode{
		node:    n,
		path:    path,
		tmpPath: fpath,
		f:       tmpFile,
		ref:     ref,
	}
	if _, ok := c.openedFiles[path]; !ok {
		c.openedFiles[path] = &openedFile{
			path:    path,
			tmpPath: fpath,
			ref:     ref,
			fds:     1,
		}
	} else {
		c.openedFiles[path].fds++
	}

	return newLoopbackFile(nodefs.NewLoopbackFile(tmpFile), wnode), nil
}

func (c *Cache) getNode(ref, path string) (*Node, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.reconnectPending && (c.liveUpdate || c.snapshot) {
		if n, ok := c.nodeIndex[path]; ok {
			return n, nil
		}
	}
	node := &Node{}
	if err := kvs.Client().GetJSON("/api/filetree/fs/fs/"+ref+"/"+path, nil, &node); err != nil {
		return nil, err
	}
	if !c.reconnectPending && (c.liveUpdate || c.snapshot) {
		c.nodeIndex[path] = node
	}
	return node, nil

}

type FileSystem struct {
	ref        string
	debug      bool
	ro         bool
	snapshot   bool
	liveUpdate bool
	mu         sync.Mutex
}

func NewFileSystem(ref string, debug, rw, snapshot, liveUpdate bool) pathfs.FileSystem {
	return &FileSystem{
		ref:        ref,
		debug:      debug,
		ro:         !rw,
		snapshot:   snapshot,
		liveUpdate: liveUpdate,
	}
}

func (fs *FileSystem) String() string {
	return fmt.Sprintf("FileSystem(%s)", fs.ref)
}

func (fs *FileSystem) SetDebug(debug bool) {
	fs.debug = debug
}

func (*FileSystem) StatFs(name string) *fuse.StatfsOut {
	blocks := uint64(9999999999)
	return &fuse.StatfsOut{
		Bfree:  blocks,
		Ffree:  blocks,
		Bavail: blocks,
		Blocks: blocks,
	}
}

func (*FileSystem) OnMount(nodeFs *pathfs.PathNodeFs) {}

func (*FileSystem) OnUnmount() {}

func (fs *FileSystem) Utimens(path string, a *time.Time, m *time.Time, context *fuse.Context) fuse.Status {
	return fuse.EPERM
}

func (fs *FileSystem) GetAttr(name string, context *fuse.Context) (a *fuse.Attr, code fuse.Status) {
	mu.Lock()
	defer mu.Unlock()
	if fs.debug {
		log.Printf("OP Getattr %s", name)
	}

	// fullPath := fmt.Sprintf("%x", blake2b.Sum256([]byte(name)))

	// var err error = nil
	// st := syscall.Stat_t{}
	// if name == "" {
	// 	// When GetAttr is called for the toplevel directory, we always want
	// 	// to look through symlinks.
	// 	err = syscall.Stat(fullPath, &st)
	// } else {
	// 	err = syscall.Lstat(fullPath, &st)
	// }

	// if err == nil {
	// 	a = &fuse.Attr{}
	// 	a.FromStat(&st)
	// 	return a, fuse.OK
	// }

	// if _, ok := cache.negNodeIndex[name]; ok {
	// 	if _, ok2 := cache.openedFiles[name]; !ok2 {
	// 		return nil, fuse.ENOENT
	// 	}
	// }
	node, err := cache.getNode(fs.ref, name)
	// fmt.Printf("node=%+v\n", node)
	if err != nil || node.IsFile() {
		// TODO(tsileo): proper error checking

		cache.mu.Lock()
		defer cache.mu.Unlock()
		openedFile, ok := cache.openedFiles[name]
		if ok && false { // FIXME(tsileo)
			f, err := os.Open(openedFile.tmpPath)
			if err != nil {
				return nil, fuse.EIO
			}
			stat, err := f.Stat()
			if err != nil {
				return nil, fuse.EIO
			}
			return fuse.ToAttr(stat), fuse.OK
			// return &fuse.Attr{
			// 	Mode: fuse.S_IFREG | 0644,
			// 	// Mode:  uint32(stat.Mode()),
			// 	Size:  uint64(stat.Size()),
			// 	Mtime: uint64(stat.ModTime().Unix()),
			// }, fuse.OK
		}

		if node == nil {
			cache.negNodeIndex[name] = struct{}{}
			return nil, fuse.ENOENT
		}
	}
	if node.IsDir() {
		out := &fuse.Attr{
			Mode:  fuse.S_IFDIR | 0755,
			Mtime: node.Mtime(),
			Atime: node.Mtime(),
			Ctime: node.Mtime(),
			Owner: *owner,
		}
		return out, fuse.OK
	}
	// fmt.Printf("returning size:%d\n", node.Size)
	out := &fuse.Attr{
		Mode:  fuse.S_IFREG | 0644,
		Size:  uint64(node.Size),
		Mtime: node.Mtime(),
		Ctime: node.Mtime(),
		Atime: node.Mtime(),
		Owner: *owner,
	}
	log.Printf("OUT=%+v\n", out)
	return out, fuse.OK
}

func (fs *FileSystem) SetAttr(input *fuse.SetAttrIn, out *fuse.AttrOut) (code fuse.Status) {
	log.Printf("OP SetAttr %+v %+v", input, out)
	return fuse.EPERM
}

func (fs *FileSystem) OpenDir(name string, context *fuse.Context) (stream []fuse.DirEntry, status fuse.Status) {
	if fs.debug {
		log.Printf("OP OpenDir %s", name)
	}
	node, err := cache.getNode(fs.ref, name)
	if err != nil {
		return nil, fuse.ENOENT
	}
	if !node.IsDir() {
		return nil, fuse.ENOTDIR
	}

	output := []fuse.DirEntry{}
	if node.Children != nil {
		for _, child := range node.Children {
			if child.IsFile() {
				output = append(output, fuse.DirEntry{Name: child.Name, Mode: fuse.S_IFREG})
			} else {
				output = append(output, fuse.DirEntry{Name: child.Name, Mode: fuse.S_IFDIR})
			}
		}
	}
	return output, fuse.OK
}

func (fs *FileSystem) Open(name string, flags uint32, context *fuse.Context) (fuseFile nodefs.File, status fuse.Status) {
	if fs.debug {
		log.Printf("OP Open %s write=%v\n", name, flags&fuse.O_ANYWRITE != 0)
	}

	// p := fmt.Sprintf("%x", blake2b.Sum256([]byte(name)))
	// f, err := os.OpenFile(p, int(flags), 0)
	// if err != nil {
	// 	return nil, fuse.ToStatus(err)
	// }
	// return nodefs.NewLoopbackFile(f), fuse.OK
	_, alreadyOpen := cache.openedFiles[name]

	if flags&fuse.O_ANYWRITE != 0 || alreadyOpen {
		if fs.ro {
			return nil, fuse.EROFS
		}

		f, err := cache.newWritableNode(fs.ref, name, 0755)
		if err != nil {
			return nil, fuse.EIO
		}
		fmt.Print("before open return\n")
		return f, fuse.OK
	}
	node, err := cache.getNode(fs.ref, name)
	if err != nil {
		return nil, fuse.ENOENT
	}
	f, err := NewFile(fs, name, node)
	if err != nil {
		return nil, fuse.EIO
	}

	return f, fuse.OK
}

func (fs *FileSystem) Chmod(path string, mode uint32, context *fuse.Context) (code fuse.Status) {
	return fuse.EPERM
}

func (fs *FileSystem) Chown(path string, uid uint32, gid uint32, context *fuse.Context) (code fuse.Status) {
	return fuse.EPERM
}

func (fs *FileSystem) Truncate(path string, offset uint64, context *fuse.Context) (code fuse.Status) {
	// Will be called on the File instead
	panic("should never be called")
	return fuse.EPERM
}

func (fs *FileSystem) Readlink(name string, context *fuse.Context) (out string, code fuse.Status) {
	return "", fuse.ENOSYS
}

func (fs *FileSystem) Mknod(name string, mode uint32, dev uint32, context *fuse.Context) (code fuse.Status) {
	return fuse.ENOSYS
}

func (fs *FileSystem) Mkdir(path string, mode uint32, context *fuse.Context) (code fuse.Status) {
	if fs.debug {
		log.Printf("OP Mkdir %s", path)
	}
	if fs.ro {
		return fuse.EPERM
	}

	node := map[string]interface{}{
		"type":    "dir",
		"name":    filepath.Base(path),
		"version": "1",
	}
	d := filepath.Dir(path)
	if d == "." {
		d = ""
	}
	// fmt.Printf("node=%+v\n", node)
	js, err := json.Marshal(node)
	if err != nil {
		return fuse.EIO
	}
	resp, err := kvs.Client().DoReq("PATCH", "/api/filetree/fs/fs/"+fs.ref+"/"+d, nil, bytes.NewReader(js))
	if err != nil || resp.StatusCode != 200 {
		return fuse.EIO
	}
	return fuse.OK
}

// Don't use os.Remove, it removes twice (unlink followed by rmdir).
func (fs *FileSystem) Unlink(name string, context *fuse.Context) (code fuse.Status) {
	if fs.debug {
		log.Printf("OP Unlink %s", name)
	}
	if fs.ro {
		return fuse.EPERM
	}

	_, err := cache.getNode(fs.ref, name)
	if err != nil {
		return fuse.ENOENT
	}

	resp, err := kvs.Client().DoReq("DELETE", "/api/filetree/fs/fs/"+fs.ref+"/"+name, nil, nil)
	if err != nil || resp.StatusCode != 204 {
		return fuse.EIO
	}

	return fuse.OK
}

func (fs *FileSystem) Rmdir(name string, context *fuse.Context) (code fuse.Status) {
	if fs.debug {
		log.Printf("OP Rmdir %s", name)
	}
	if fs.ro {
		return fuse.EPERM
	}
	node, err := cache.getNode(fs.ref, name)
	if err != nil {
		return fuse.ENOENT
	}
	if !node.IsDir() {
		return fuse.ENOTDIR
	}
	// Ensure the children check works
	if node.Children != nil && len(node.Children) > 0 {
		return fuse.Status(syscall.ENOTEMPTY)
	}

	resp, err := kvs.Client().DoReq("DELETE", "/api/filetree/fs/fs/"+fs.ref+"/"+name, nil, nil)
	if err != nil || resp.StatusCode != 204 {
		return fuse.EIO
	}

	return fuse.OK
}

func (fs *FileSystem) Symlink(pointedTo string, linkName string, context *fuse.Context) (code fuse.Status) {
	if fs.ro {
		return fuse.EPERM
	}
	return fuse.ENOSYS // FIXME(tsileo): return ENOSYS when needed in other calls
}

func (fs *FileSystem) Rename(oldPath string, newPath string, context *fuse.Context) (codee fuse.Status) {
	if fs.debug {
		log.Printf("OP Rename %s %s", oldPath, newPath)
	}
	if fs.ro {
		return fuse.EPERM
	}
	node, err := cache.getNode(fs.ref, oldPath)
	if err != nil {
		return fuse.ENOENT
	}

	// First, we remove the old path
	resp, err := kvs.Client().DoReq("DELETE", "/api/filetree/fs/fs/"+fs.ref+"/"+oldPath, nil, nil)
	if err != nil || resp.StatusCode != 204 {
		return fuse.EIO
	}

	// Next, we re-add it to its dest
	h := map[string]string{
		"BlobStash-Filetree-Patch-Ref":  node.Ref,
		"BlobStash-Filetree-Patch-Name": filepath.Base(newPath),
	}
	dest := filepath.Dir(newPath)
	if dest == "." {
		dest = ""
	}

	resp, err = kvs.Client().DoReq("PATCH", "/api/filetree/fs/fs/"+fs.ref+"/"+dest, h, nil)
	if err != nil || resp.StatusCode != 200 {
		return fuse.EIO
	}

	return fuse.OK
}

func (fs *FileSystem) Link(orig string, newName string, context *fuse.Context) (code fuse.Status) {
	if fs.ro {
		return fuse.EPERM
	}
	return fuse.ENOSYS
}

func (fs *FileSystem) Access(name string, mode uint32, context *fuse.Context) (code fuse.Status) {
	if fs.debug {
		log.Printf("OP Access %s", name)
	}
	return fuse.OK
}

func (fs *FileSystem) Create(path string, flags uint32, mode uint32, context *fuse.Context) (fuseFile nodefs.File, code fuse.Status) {
	if fs.debug {
		log.Printf("OP Create %s", path)
	}
	if fs.ro {
		return nil, fuse.EPERM
	}
	if _, ok := cache.negNodeIndex[path]; ok {
		delete(cache.negNodeIndex, path)
	}

	// f, err := os.OpenFile(fmt.Sprintf("%x", blake2b.Sum256([]byte(path))), int(flags)|os.O_CREATE, os.FileMode(mode))
	// return nodefs.NewLoopbackFile(f), fuse.ToStatus(err)

	f, err := cache.newWritableNode(fs.ref, path, mode)
	if err != nil {
		return nil, fuse.EIO
	}
	f.Flush()
	// if err := f.Flush(); err != nil {
	// return nil, fuse.EIO
	// }
	fmt.Print("before open return\n")
	return f, fuse.OK
}

func (fs *FileSystem) GetXAttr(name string, attr string, context *fuse.Context) ([]byte, fuse.Status) {
	if fs.debug {
		log.Printf("OP GetXAttr %s", name)
	}
	node, err := cache.getNode(fs.ref, name)
	if err != nil {
		return nil, fuse.ENOENT
	}
	if attr == "node.ref" {
		return []byte(node.Ref), fuse.OK
	}

	if strings.HasPrefix(attr, "node.metadata.") && node.Metadata != nil {
		if v, ok := node.Metadata[attr[14:]]; ok {
			return []byte(fmt.Sprintf("%v", v)), fuse.OK
		}
	}

	return nil, fuse.ENOATTR
}

func (fs *FileSystem) SetXAttr(name string, attr string, data []byte, flags int, context *fuse.Context) fuse.Status {
	if fs.debug {
		log.Printf("OP SetXAttr %s", name)
	}
	return fuse.EPERM
}

func (fs *FileSystem) ListXAttr(name string, context *fuse.Context) ([]string, fuse.Status) {
	if fs.debug {
		log.Printf("OP ListXAttr %s", name)
	}

	fullPath := fmt.Sprintf("%x", blake2b.Sum256([]byte(name)))
	if list, err := xattr.List(fullPath); err == nil {
		return list, fuse.OK
	}

	node, err := cache.getNode(fs.ref, name)
	if err != nil {
		return nil, fuse.ENOENT
	}
	out := []string{"node.ref"}
	if node.Metadata != nil {
		for k, _ := range node.Metadata {
			out = append(out, fmt.Sprintf("node.metadata.%s", k))
		}
	}
	return out, fuse.OK
}

func (fs *FileSystem) RemoveXAttr(name string, attr string, context *fuse.Context) fuse.Status {
	if fs.debug {
		log.Printf("OP RemoveXAttr %s", name)
	}
	return fuse.EPERM
}

func NewFile(fs *FileSystem, path string, node *Node) (*File, error) {
	ctx := context.TODO()
	blob, err := cache.Get(ctx, node.Ref)
	if err != nil {
		return nil, err
	}
	meta, err := rnode.NewNodeFromBlob(node.Ref, blob)
	if err != nil {
		return nil, err
	}

	// If the file is too big, we don't want to fill the whole local blob cache with blob of a single file,
	// so we create a tiny in-memory cache just for the lifetime of the file
	var fcache *lru.Cache
	if len(meta.Refs) > 3 {
		fcache, err = lru.New(5)
		if err != nil {
			return nil, err
		}
	}
	r := filereader.NewFile(ctx, cache, meta, fcache)
	return &File{
		node: node,
		fs:   fs,
		path: path,
		r:    r,
	}, nil
}

type File struct {
	nodefs.File
	node  *Node
	r     *filereader.File
	inode *nodefs.Inode
	path  string
	fs    *FileSystem
}

func (f *File) SetInode(inode *nodefs.Inode) {
	f.inode = inode
}

func (f *File) String() string {
	return fmt.Sprintf("File(%s, %s)", f.path, f.node.Ref)
}

func (f *File) Write(data []byte, off int64) (uint32, fuse.Status) {
	return 0, fuse.EROFS
}

func (f *File) Read(buf []byte, off int64) (res fuse.ReadResult, code fuse.Status) {
	if f.fs.debug {
		log.Printf("OP Read %v", f)
	}
	if _, err := f.r.ReadAt(buf, off); err != nil {
		return nil, fuse.EIO
	}
	return fuse.ReadResultData(buf), fuse.OK
}

func (f *File) Release() {
	if f.fs.debug {
		log.Printf("OP Release %v", f)
	}
	f.r.Close()
}

func (f *File) Flush() fuse.Status {
	return fuse.OK
}

func (f *File) GetAttr(a *fuse.Attr) fuse.Status {
	if f.fs.debug {
		log.Printf("OP Getattr %v", f)
	}
	a.Mode = fuse.S_IFREG | 0644
	a.Size = uint64(f.node.Size)
	a.Mtime = f.node.Mtime()
	a.Ctime = f.node.Mtime()
	a.Atime = f.node.Mtime()
	a.Owner = *owner
	return fuse.OK
}
