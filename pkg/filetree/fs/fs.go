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

	bcache "a4.io/blobstash/pkg/cache"
	"a4.io/blobstash/pkg/client/clientutil"
	"a4.io/blobstash/pkg/client/kvstore"
	"a4.io/blobstash/pkg/client/oplog"
	rnode "a4.io/blobstash/pkg/filetree/filetreeutil/node"
	"a4.io/blobstash/pkg/filetree/reader/filereader"
)

// TODO(tsileo): test Server.Notify(path)!

// filetree/reader to

var kvs *kvstore.KvStore
var cache *Cache

type FSUpdateEvent struct {
	Name     string `json:"fs_name"`
	Path     string `json:"fs_path"`
	Ref      string `json:"node_ref"`
	Type     string `json:"node_type"`
	Time     int64  `json:"event_time"`
	Hostname string `json:"event_hostname"`
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
	flag.Parse()
	if flag.NArg() < 2 {
		fmt.Fprintf(os.Stderr, "usage: %s MOUNTPOINT REF\n", os.Args[0])
		os.Exit(2)
	}

	var err error
	cache, err = newCache("fs_cache")
	if err != nil {
		fmt.Printf("failed to setup cache: %v\n", err)
		os.Exit(1)
	}
	kvopts := kvstore.DefaultOpts().SetHost(os.Getenv("BLOBS_API_HOST"), os.Getenv("BLOBS_API_KEY"))
	kvopts.SnappyCompression = false
	kvs = kvstore.New(kvopts)

	oplogClient := oplog.New(kvopts)
	ops := make(chan *oplog.Op)
	root := NewFileSystem(flag.Arg(1))

	opts := &nodefs.Options{
		Debug: *debug,
	}
	nfs := pathfs.NewPathNodeFs(root, nil)
	// state, _, err := nodefs.MountRoot(flag.Arg(0), nfs.Root(), opts)

	conn := nodefs.NewFileSystemConnector(nfs.Root(), opts)

	// XXX(tsileo): different options on READ ONLY mode
	mountOpts := fuse.MountOptions{
		Options: []string{
			// FIXME(tsileo): no more nolocalcaches and use notify instead for linux
			//"nolocalcaches",
			"defer_permissions",
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

	go state.Serve()
	log.Printf("mounted successfully")

	go func() {
		if err := oplogClient.Notify(ops); err != nil {
			panic(err)
		}
	}()
	go func() {
		for op := range ops {
			if op.Event == "filetree" {
				fmt.Printf("op=%+v\n", op)
				evt := EventFromJSON(op.Data)
				fmt.Printf("evt=%+v\n", evt)
				// switch evt.Type {
				// case "file-updated":
				if err := nfs.Notify(evt.Path); err != fuse.OK {
					fmt.Printf("failed to notify=%+v\n", err)
				}
				// default:
				// panic("unknown event type")
				// }
			}
		}
	}()

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
	if status := f.File.Flush(); status != fuse.OK {
		return status
	}
	fmt.Printf("CUSTOM FLUSH")
	// TODO(tsileo): in the future, chunk **big** files locally to prevent sending everyting

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
	openedFiles map[string]*openedFile
	nodeIndex   map[string]*Node
	mu          sync.Mutex
	path        string
	blobsCache  *bcache.Cache
}

func newCache(path string) (*Cache, error) {
	if err := os.RemoveAll(path); err != nil {
		return nil, err
	}
	// if _, err := os.Stat(path); os.IsNotExist(err) {
	if err := os.Mkdir(path, 0700); err != nil {
		return nil, err
	}
	// }
	blobsCache, err := bcache.New(".", "blobs.cache", 256<<20)
	if err != nil {
		return nil, err
	}

	return &Cache{
		openedFiles: map[string]*openedFile{},
		nodeIndex:   map[string]*Node{},
		path:        path,
		blobsCache:  blobsCache,
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

func (c *Cache) newWritableNode(ref, path string) (nodefs.File, error) {
	// XXX(tsileo): use hash for consistent filename, only load the node into the file if it just got created
	// or return a fd to the already present file
	// FIXME(tsileo): use a custom cache dir
	fname := fmt.Sprintf("%x", blake2b.Sum256([]byte(fmt.Sprintf("%s:%s", path))))
	fpath := filepath.Join(c.path, fname)

	var err error
	var tmpFile *os.File
	var shouldLoad bool
	if _, err := os.Stat(path); os.IsNotExist(err) {
		// FIXME(tsileo): should we set the modtime of the original node?
		tmpFile, err = os.Create(fpath)
		shouldLoad = true
	} else {
		tmpFile, err = os.OpenFile(fpath, os.O_RDWR, 0755)
	}
	if err != nil {
		return nil, err
	}

	var n *Node

	// Copy the original content if the node already exists
	n, err = c.getNode(ref, path)
	switch err {
	case nil:
		if !shouldLoad {
			break
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
	// XXX(tsileo): HEAD requests to check if the ref at path is the same
	// if n, ok := c.nodeIndex[path]; ok {
	// return n, nil
	// }
	node := &Node{}
	if err := kvs.Client().GetJSON("/api/filetree/fs/fs/"+ref+"/"+path, nil, &node); err != nil {
		return nil, err
	}
	c.nodeIndex[path] = node
	return node, nil

}

type FileSystem struct {
	ref   string
	debug bool
	ro    bool
}

func NewFileSystem(ref string) pathfs.FileSystem {
	return &FileSystem{
		ref: ref,
	}
}

func (fs *FileSystem) String() string {
	return fmt.Sprintf("FileSystem(%s)", fs.ref)
}

func (fs *FileSystem) SetDebug(debug bool) {
	fs.debug = debug
}

func (*FileSystem) StatFs(name string) *fuse.StatfsOut {
	// out := &fuse.StatfsOut{}
	return nil
}

func (*FileSystem) OnMount(nodeFs *pathfs.PathNodeFs) {}

func (*FileSystem) OnUnmount() {}

func (fs *FileSystem) Utimens(path string, a *time.Time, m *time.Time, context *fuse.Context) fuse.Status {
	return fuse.ENOSYS
}

func (fs *FileSystem) GetAttr(name string, context *fuse.Context) (a *fuse.Attr, code fuse.Status) {
	log.Printf("OP Getattr %s", name)
	node, err := cache.getNode(fs.ref, name)
	// fmt.Printf("node=%+v\n", node)
	if err != nil || node.Type == "file" {
		// TODO(tsileo): proper error checking

		cache.mu.Lock()
		defer cache.mu.Unlock()
		openedFile, ok := cache.openedFiles[name]
		if ok {
			f, err := os.Open(openedFile.tmpPath)
			if err != nil {
				return nil, fuse.EIO
			}
			stat, err := f.Stat()
			if err != nil {
				return nil, fuse.EIO
			}
			return &fuse.Attr{
				Mode:  fuse.S_IFREG | 0644,
				Size:  uint64(stat.Size()),
				Mtime: node.Mtime(),
			}, fuse.OK
		}

		if node == nil {
			return nil, fuse.ENOENT
		}
	}
	if node.Type == "dir" {
		return &fuse.Attr{
			Mode:  fuse.S_IFDIR | 0755,
			Mtime: node.Mtime(),
		}, fuse.OK
	}
	// fmt.Printf("returning size:%d\n", node.Size)
	return &fuse.Attr{
		Mode:  fuse.S_IFREG | 0644,
		Size:  uint64(node.Size),
		Mtime: node.Mtime(),
	}, fuse.OK
}

func (fs *FileSystem) SetAttr(input *fuse.SetAttrIn, out *fuse.AttrOut) (code fuse.Status) {
	if fs.ro {
		return fuse.EROFS
	}
	return fuse.ENOSYS
}

func (fs *FileSystem) OpenDir(name string, context *fuse.Context) (stream []fuse.DirEntry, status fuse.Status) {
	log.Printf("OP OpenDir %s", name)
	node, err := cache.getNode(fs.ref, name)
	if err != nil {
		return nil, fuse.ENOENT
	}
	if node.Type == "file" {
		return nil, fuse.ENOTDIR
	}

	output := []fuse.DirEntry{}
	if node.Children != nil {
		for _, child := range node.Children {
			if child.Type == "file" {
				output = append(output, fuse.DirEntry{Name: child.Name, Mode: fuse.S_IFREG})
			} else {
				output = append(output, fuse.DirEntry{Name: child.Name, Mode: fuse.S_IFDIR})
			}
		}
	}
	return output, fuse.OK
}

func (fs *FileSystem) Open(name string, flags uint32, context *fuse.Context) (fuseFile nodefs.File, status fuse.Status) {
	log.Printf("OP Open %s write=%v\n", name, flags&fuse.O_ANYWRITE != 0)
	// FIXME(tsileo): also return a writable node if there's already a writable file open
	if flags&fuse.O_ANYWRITE != 0 {
		if fs.ro {
			return nil, fuse.EROFS
		}

		f, err := cache.newWritableNode(fs.ref, name)
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
	if fs.ro {
		return fuse.EROFS
	}
	return fuse.ENOSYS
}

func (fs *FileSystem) Chown(path string, uid uint32, gid uint32, context *fuse.Context) (code fuse.Status) {
	if fs.ro {
		return fuse.EROFS
	}
	return fuse.ENOSYS
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
	if fs.ro {
		return fuse.EROFS
	}
	return fuse.ENOSYS
}

func (fs *FileSystem) Mkdir(path string, mode uint32, context *fuse.Context) (code fuse.Status) {
	log.Printf("OP Mkdir %s", path)
	if fs.ro {
		return fuse.EROFS
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
	log.Printf("OP Unlink %s", name)
	if fs.ro {
		return fuse.EROFS
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
	log.Printf("OP Rmdir %s", name)
	if fs.ro {
		return fuse.EROFS
	}
	node, err := cache.getNode(fs.ref, name)
	if err != nil {
		return fuse.ENOENT
	}
	if node.Type != "dir" {
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
		return fuse.EROFS
	}
	return fuse.ENOSYS // FIXME(tsileo): return ENOSYS when needed in other calls
}

func (fs *FileSystem) Rename(oldPath string, newPath string, context *fuse.Context) (codee fuse.Status) {
	log.Printf("OP Rename %s %s", oldPath, newPath)
	if fs.ro {
		return fuse.EROFS
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
		return fuse.EROFS
	}
	return fuse.ENOSYS
}

func (fs *FileSystem) Access(name string, mode uint32, context *fuse.Context) (code fuse.Status) {
	log.Printf("OP Access %s", name)
	return fuse.OK
}

func (fs *FileSystem) Create(path string, flags uint32, mode uint32, context *fuse.Context) (fuseFile nodefs.File, code fuse.Status) {
	log.Printf("OP Create %s", path)
	if fs.ro {
		return nil, fuse.EROFS
	}
	f, err := cache.newWritableNode(fs.ref, path)
	if err != nil {
		return nil, fuse.EIO
	}
	fmt.Print("before open return\n")
	return f, fuse.OK
}

func (fs *FileSystem) GetXAttr(name string, attr string, context *fuse.Context) ([]byte, fuse.Status) {
	log.Printf("OP GetXAttr %s", name)
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

	return nil, fuse.ENODATA
}

func (fs *FileSystem) SetXAttr(name string, attr string, data []byte, flags int, context *fuse.Context) fuse.Status {
	if fs.ro {
		return fuse.EROFS
	}
	return fuse.ENOSYS
}

func (fs *FileSystem) ListXAttr(name string, context *fuse.Context) ([]string, fuse.Status) {
	log.Printf("OP ListXAttr %s", name)
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
	if fs.ro {
		return fuse.EROFS
	}
	return fuse.ENOSYS
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
	r := filereader.NewFile(ctx, cache, meta, nil)
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
	log.Printf("OP Read %v", f)
	if _, err := f.r.ReadAt(buf, off); err != nil {
		return nil, fuse.EIO
	}
	return fuse.ReadResultData(buf), fuse.OK
}

func (f *File) Release() {
	log.Printf("OP Release %v", f)
	f.r.Close()
}

func (f *File) Flush() fuse.Status {
	return fuse.OK
}

func (f *File) GetAttr(a *fuse.Attr) fuse.Status {
	log.Printf("OP Getattr %v", f)
	a.Mode = fuse.S_IFREG | 0644
	a.Size = uint64(f.node.Size)
	a.Mtime = f.node.Mtime()
	return fuse.OK
}
