package main

// import "a4.io/blobstash/pkg/filetree/fs"

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	_ "io/ioutil"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/dchest/blake2b"
	"github.com/dustin/go-humanize"
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
	"github.com/hashicorp/golang-lru"
	_ "github.com/pkg/xattr"

	bcache "a4.io/blobstash/pkg/cache"
	"a4.io/blobstash/pkg/client/blobstore"
	"a4.io/blobstash/pkg/client/clientutil"
	"a4.io/blobstash/pkg/client/kvstore"
	"a4.io/blobstash/pkg/config/pathutil"
	rnode "a4.io/blobstash/pkg/filetree/filetreeutil/node"
	"a4.io/blobstash/pkg/filetree/reader/filereader"
	"a4.io/blobstash/pkg/filetree/writer"
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
var bs *blobstore.BlobStore
var cache *Cache
var owner *fuse.Owner
var mu sync.Mutex

func main() {
	// Scans the arg list and sets up flags
	debug := flag.Bool("debug", false, "print debugging messages.")
	debugTicker := flag.Int("debug-ticker", 0, "dump stats every x seconds in the logs.")
	resetCache := flag.Bool("reset-cache", false, "remove the local cache before starting.")
	flag.Parse()
	if flag.NArg() < 2 {
		fmt.Fprintf(os.Stderr, "usage: %s MOUNTPOINT REF\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(2)
	}
	mountpoint := flag.Arg(0)
	ref := flag.Arg(1)

	// Cache setup, follow XDG spec
	cacheDir := filepath.Join(pathutil.CacheDir(), "fs", fmt.Sprintf("%s_%s", mountpoint, ref))

	if _, err := os.Stat(cacheDir); err != nil {
		if os.IsNotExist(err) {
			if err := os.MkdirAll(cacheDir, 0700); err != nil {
				fmt.Printf("failed to create cache dir: %v\n", err)
				os.Exit(1)
			}
		}

	} else {
		if *resetCache {
			if err := os.RemoveAll(cacheDir); err != nil {
				fmt.Printf("failed to reset cache: %v\n", err)
				os.Exit(1)
			}
			if err := os.MkdirAll(cacheDir, 0700); err != nil {
				fmt.Printf("failed to re-create cache dir: %v\n", err)
				os.Exit(1)
			}
		}
	}

	var err error
	cache, err = newCache(cacheDir)
	if err != nil {
		fmt.Printf("failed to setup cache at %s: %v\n", cacheDir, err)
		os.Exit(1)
	}

	// Setup the clients for BlobStash
	kvopts := kvstore.DefaultOpts().SetHost(os.Getenv("BLOBS_API_HOST"), os.Getenv("BLOBS_API_KEY"))
	kvopts.SnappyCompression = false
	kvs = kvstore.New(kvopts)
	bs = blobstore.New(kvopts)

	authOk, err := kvs.Client().CheckAuth(context.TODO())
	if err != nil {
		fmt.Printf("failed to contact BlobStash: %v\n", err)
		os.Exit(1)
	}

	if !authOk {
		fmt.Printf("bad API key\n")
		os.Exit(1)
	}

	root, err := NewFileSystem(flag.Arg(1), flag.Arg(0), *debug, cacheDir)
	if err != nil {
		fmt.Printf("failed to initialize filesystem: %v\n", err)
		os.Exit(1)
	}
	opts := &nodefs.Options{
		Debug:           false, // *debug,
		EntryTimeout:    0 * time.Second,
		AttrTimeout:     0 * time.Second,
		NegativeTimeout: 0 * time.Second,
	}

	// Optional periodic dump of the stats, enabled via -debug-ticker x, where x is the interval in seconds between the dumps
	if *debugTicker > 0 {
		ticker := time.NewTicker(time.Duration(*debugTicker) * time.Second)
		go func() {
			for _ = range ticker.C {
				log.Printf("DEBUG:\n[cache]\ncache.hits=%d cache.hits_pct=%.2f cache.reqs=%d cache.cached=%d cache.size=%v", cache.hits, float64(cache.hits)*100.0/float64(cache.reqs), cache.reqs, cache.cached, humanize.Bytes(uint64(cache.blobsCache.Size())))
				fmt.Printf("[fs]\nfs.fds=%d fs.rw_files=%d fs.eio=%d fs.uptime=%v\n", root.openedFds, len(root.rwLayer.cache), root.eioCount, time.Since(root.startedAt))
				fmt.Printf("[blobstash]\nblobstash.file_uploaded=%d blobstash.errors=%d blobstash.fs_reqs=%d\n", root.uploadedFiles, root.blobstashErrors, cache.blobstashFSreqs)
			}
		}()
	}

	nfs := pathfs.NewPathNodeFs(root, nil)
	// state, _, err := nodefs.MountRoot(flag.Arg(0), nfs.Root(), opts)

	conn := nodefs.NewFileSystemConnector(nfs.Root(), opts)

	// XXX(tsileo): different options on READ ONLY mode
	mountOpts := fuse.MountOptions{
		// AllowOther: true,
		Options: []string{
			// FIXME(tsileo): no more nolocalcaches and use notify instead for linux
			"noatime",
		},
		FsName: "blobfs",
		Name:   ref,
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
	go state.Serve()
	log.Println("mounted successfully")

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
	cache.Close()
	log.Println("unmounted")
	os.Exit(0)
}

type rwLayer struct {
	path  string
	cache map[string]*RWFileMeta
	index map[string][]*RWFileMeta
}

func (rl *rwLayer) Path(name string) string {
	name = fmt.Sprintf("%x", sha1.Sum([]byte(name)))
	return filepath.Join(rl.path, name)

}

func (rl *rwLayer) Exists(name string) (string, bool, error) {
	path := rl.Path(name)
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return path, false, nil
		}
		return "", false, err
	}
	return path, true, nil
}

func (rl *rwLayer) GetAttr(name string, context *fuse.Context) (a *fuse.Attr, code fuse.Status, err error) {
	path, rwExists, err := rl.Exists(name)
	if err != nil {
		log.Printf("OP GetAttr %s rwExists err=%+v\n", name, err)
		return nil, fuse.EIO, err
	}
	if !rwExists {
		return nil, fuse.ENOENT, nil
	}
	st := syscall.Stat_t{}
	if name == "" {
		// When GetAttr is called for the toplevel directory, we always want
		// to look through symlinks.
		err = syscall.Stat(path, &st)
	} else {
		err = syscall.Lstat(path, &st)
	}

	if err != nil {
		return nil, fuse.ToStatus(err), err
	}
	a = &fuse.Attr{}
	a.FromStat(&st)
	a.Ino = 0
	a.Blocks = 0
	a.Atime = 0
	a.Atimensec = 0
	a.Mtimensec = 0
	a.Ctimensec = 0
	a.Nlink = 0
	a.Uid = 0
	a.Gid = 0
	a.Rdev = 0
	a.Blksize = 0

	//log.Printf("OUT rwlayer=%+v\n", a)
	return a, fuse.OK, nil
}

func (rl *rwLayer) OpenDir(name string, context *fuse.Context) (map[string]fuse.DirEntry, fuse.Status, error) {
	path, rwExists, err := rl.Exists(name)
	if err != nil {
		log.Printf("OP GetAttr %s rwExists err=%+v\n", name, err)
		return nil, fuse.EIO, err
	}
	if !rwExists {
		return nil, fuse.ENOENT, nil
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, fuse.EIO, err
	}
	output := map[string]fuse.DirEntry{}
	want := 500
	// output := make([]fuse.DirEntry, 0, want)
	for {
		infos, err := f.Readdir(want)
		for i := range infos {
			// workaround for https://code.google.com/p/go/issues/detail?id=5960
			if infos[i] == nil {
				continue
			}
			n := infos[i].Name()
			d := fuse.DirEntry{
				Name: n,
			}
			if s := fuse.ToStatT(infos[i]); s != nil {
				d.Mode = uint32(s.Mode)
				d.Ino = s.Ino
			} else {
				log.Printf("ReadDir entry %q for %q has no stat info\n", n, name)
			}
			output[n] = d
			// output = append(output, d)
		}
		if len(infos) < want || err == io.EOF {
			// if err == io.EOF {
			break
		}
		if err != nil {
			log.Println("Readdir() returned err:", err)
			break
		}
	}
	f.Close()
	return output, fuse.OK, nil
}

func newRWLayer(path string) (*rwLayer, error) {
	// TODO(tsileo): remove everything at startup?
	_, err := os.Stat(path)
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("failed to init rw layer: %v", err)
	}
	if err == nil {
		// Reset any rw files still here as they may be stale
		if err := os.RemoveAll(path); err != nil {
			return nil, fmt.Errorf("failed to remove/reset the rw layer dir: %v", err)
		}
	}
	if err := os.MkdirAll(path, 0700); err != nil {
		return nil, fmt.Errorf("failed to create rw layer dir: %v", err)
	}

	return &rwLayer{
		path:  path,
		cache: map[string]*RWFileMeta{},
		index: map[string][]*RWFileMeta{},
	}, nil
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

func (n *Node) Hash() string {
	return n.Metadata["blake2b-hash"].(string)
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

func (n *Node) Copy(dst io.Writer, meta *rnode.RawNode) error {
	ctx := context.TODO()
	// If the file is too big, we don't want to fill the whole local blob cache with blob of a single file,
	// so we create a tiny in-memory cache just for the lifetime of the file
	var err error
	var fcache *lru.Cache
	if len(meta.Refs) > 3 {
		fcache, err = lru.New(5)
		if err != nil {
			return err
		}
	}

	fileReader := filereader.NewFile(ctx, cache, meta, fcache)
	defer fileReader.Close()
	io.Copy(dst, fileReader)
	return nil
}

type Cache struct {
	nodeIndex    map[string]*Node
	negNodeIndex map[string]struct{}
	mu           sync.Mutex
	blobsCache   *bcache.Cache

	hits            int64
	reqs            int64
	cached          int64
	blobstashFSreqs int64
}

func newCache(path string) (*Cache, error) {
	blobsCache, err := bcache.New(path, "blobs.cache", 256<<20) // 256MB on-disk LRU cache
	if err != nil {
		return nil, err
	}

	return &Cache{
		nodeIndex:    map[string]*Node{},
		negNodeIndex: map[string]struct{}{},
		blobsCache:   blobsCache,
	}, nil
}

func (c *Cache) Close() error {
	return c.blobsCache.Close()
}

//func (c *Cache) Stat(ctx context.Context, hash string) (bool, error) {
func (c *Cache) Stat(hash string) (bool, error) {
	// FIXME(tsileo): is a local check needed, when can we skip the remote check?
	// locStat, err := c.blobsCache.Stat(hash)
	// if err != nil {
	// 	return false, err
	// }
	return bs.Stat(context.TODO(), hash)
}

// Get implements the BlobStore interface for filereader.File
func (c *Cache) Put(ctx context.Context, hash string, data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cached++

	if err := c.blobsCache.Add(hash, data); err != nil {
		return err
	}
	// FIXME(tsileo): add a stat/exist check once the data contexes is implemented
	if err := bs.Put(ctx, hash, data); err != nil {
		return nil
	}
	return nil
}

// Get implements the BlobStore interface for filereader.File
func (c *Cache) Get(ctx context.Context, hash string) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reqs++
	var err error
	cachedBlob, ok, err := c.blobsCache.Get(hash)
	if err != nil {
		return nil, fmt.Errorf("cache failed: %v", err)
	}
	var data []byte
	if ok {
		c.hits++
		data = cachedBlob
	} else {
		c.cached++
		// resp, err := kvs.Client().DoReq(ctx, "GET", "/api/blobstore/blob/"+hash, nil, nil)
		// if err != nil {
		// return nil, err
		// }
		// defer resp.Body.Close()
		// data, err = ioutil.ReadAll(resp.Body)
		data, err = bs.Get(ctx, hash)
		if err != nil {
			return nil, fmt.Errorf("failed to call blobstore: %v", err)
		}
		if err := c.blobsCache.Add(hash, data); err != nil {
			return nil, fmt.Errorf("failed to add to cache: %v", err)
		}
	}
	return data, nil
}

func (c *Cache) getNode(ref, path string) (*Node, fuse.Status) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.blobstashFSreqs++
	// if n, ok := c.nodeIndex[path]; ok {
	// return n, nil
	// }
	node := &Node{}
	if err := kvs.Client().GetJSON(context.TODO(), "/api/filetree/fs/fs/"+ref+"/"+path, nil, &node); err != nil {
		if err == clientutil.ErrNotFound {
			return nil, fuse.ENOENT
		}
		// TODO(tsileo): log error
		return nil, fuse.EIO
	}
	c.nodeIndex[path] = node
	return node, fuse.OK

}

// TODO(tsileo): TODO ticker

type FileSystem struct {
	ref         string
	debug       bool
	up          *writer.Uploader
	mu          sync.Mutex
	ro          bool
	justCreated bool
	rwLayer     *rwLayer

	startedAt time.Time

	openedFds       int64
	uploadedFiles   int64
	blobstashErrors int64
	eioCount        int64
}

func NewFileSystem(ref, mountpoint string, debug bool, cacheDir string) (*FileSystem, error) {
	fs := &FileSystem{
		startedAt: time.Now(),
		ref:       ref,
		debug:     debug,
		rwLayer:   nil,
		up:        writer.NewUploader(cache),
	}
	var err error
	fs.rwLayer, err = newRWLayer(filepath.Join(cacheDir, "rw_layer"))
	if err != nil {
		return nil, fmt.Errorf("failed to init rwLayer: %v", err)
	}

	return fs, nil
}

func (fs *FileSystem) String() string {
	return fmt.Sprintf("FileSystem(%s)", fs.ref)
}

func (fs *FileSystem) SetDebug(debug bool) {
	fs.debug = debug
}

func (*FileSystem) StatFs(name string) *fuse.StatfsOut {
	log.Println("OP StatFs")
	return &fuse.StatfsOut{}
}

func (*FileSystem) OnMount(nodeFs *pathfs.PathNodeFs) {}

func (*FileSystem) OnUnmount() {}

func (fs *FileSystem) GetAttr(name string, context *fuse.Context) (a *fuse.Attr, code fuse.Status) {
	mu.Lock()
	defer mu.Unlock()
	log.Printf("OP Getattr %s\n", name)

	// FIXME(tsileo): fix Attr.Ino/inode

	rwAttr, rwStatus, err := fs.rwLayer.GetAttr(name, context)
	switch rwStatus {
	case fuse.OK:
		return rwAttr, rwStatus
	case fuse.ENOENT:
		// do nothing
	default:
		log.Printf("failed to call rwLayer.GetAttr: %v\n", err)
		return nil, fuse.EIO
	}

	// FIXME(tsileo): check the node first, and compare with metadata for invalidation
	node, status := cache.getNode(fs.ref, name)
	//log.Printf("remote node=%+v status=%+v\n", node, status)

	if name == "" && node == nil && fs.justCreated && status == fuse.ENOENT {
		log.Printf("return justCreated attr\n")
		return &fuse.Attr{
			Mode:  fuse.S_IFDIR | 0755,
			Owner: *owner,
		}, fuse.OK
	}

	if status != fuse.OK {
		return nil, status
	}

	// if err != nil {
	// fmt.Printf("err=%+v\n", err)
	// return nil, fuse.EIO

	if node == nil {
		cache.negNodeIndex[name] = struct{}{}
		return nil, fuse.ENOENT
	}

	if node.IsDir() {
		out := &fuse.Attr{
			Mode:  fuse.S_IFDIR | 0755,
			Ctime: node.Mtime(),
			Mtime: node.Mtime(),
			Owner: *owner,
		}
		return out, fuse.OK
	}
	// fmt.Printf("returning size:%d\n", node.Size)
	out := &fuse.Attr{
		Mode:  fuse.S_IFREG | 0644,
		Size:  uint64(node.Size),
		Ctime: node.Mtime(),
		Mtime: node.Mtime(),
		Owner: *owner,
	}
	return out, fuse.OK
}

// func (fs *FileSystem) SetAttr(input *fuse.SetAttrIn, out *fuse.AttrOut) (code fuse.Status) {
// log.Printf("OP SetAttr %+v %+v", input, out)
// return fuse.EPERM
// }

func (fs *FileSystem) OpenDir(name string, context *fuse.Context) (stream []fuse.DirEntry, status fuse.Status) {
	log.Printf("OP OpenDir %s\n", name)

	var err error
	var index map[string]fuse.DirEntry
	// The real directory is the 1st layer, if a file exists locally as a file it will show up instead of the remote version
	//if !fs.ro {
	index, status, err = fs.rwLayer.OpenDir(name, context)
	switch status {
	case fuse.OK, fuse.ENOENT:
	default:
		log.Printf("failed to call rwLayer.OpenDir: %v\n", err)
		return nil, fuse.EIO
	}

	if index == nil {
		index = map[string]fuse.DirEntry{}
	}

	// Now take a look a the remote node
	node, status := cache.getNode(fs.ref, name)
	log.Printf("OP OpenDir %s remote node=%+v status=%+v\n", name, node, status)
	switch status {
	case fuse.OK:
	case fuse.ENOENT:
		if name == "" {
			fs.justCreated = true
		}
	default:
		return nil, status
	}
	if node != nil {
		if !node.IsDir() {
			return nil, fuse.ENOTDIR
		}

		if node.Children != nil {
			for _, child := range node.Children {
				// Skip the remote node if it's already present locally
				if _, ok := index[child.Name]; ok {
					continue
				}
				mode := fuse.S_IFDIR
				if child.IsFile() {
					mode = fuse.S_IFREG
				}
				index[child.Name] = fuse.DirEntry{Name: child.Name, Mode: uint32(mode)}
			}
		}
	}
	output := []fuse.DirEntry{}
	for _, dirEntry := range index {
		output = append(output, dirEntry)
	}
	log.Printf("OpenDir OK %d children\n", len(output))
	return output, fuse.OK
}

func (fs *FileSystem) removePathFromIndex(path string) {
	parent := filepath.Base(path)
	if i, ok := fs.rwLayer.index[parent]; ok {
		newIndex := []*RWFileMeta{}
		for _, m := range i {
			if m.Path == path {
				continue
			}
			newIndex = append(newIndex, m)
		}
		fs.rwLayer.index[parent] = newIndex
		delete(fs.rwLayer.cache, path)
	}
}

func (fs *FileSystem) rwIndex(f *RWFile) {
	parent := filepath.Base(f.path)
	meta := &RWFileMeta{Node: f.node, Path: f.path}
	if i, ok := fs.rwLayer.index[parent]; ok {
		var found bool
		for _, m := range i {
			if m.Path == meta.Path {
				found = true
			}
		}
		if !found {
			i = append(i, meta)
			fs.rwLayer.cache[f.path] = meta
		}
	} else {
		fs.rwLayer.index[parent] = []*RWFileMeta{meta}
		fs.rwLayer.cache[f.path] = meta
	}
}

func (fs *FileSystem) Open(name string, flags uint32, context *fuse.Context) (fuseFile nodefs.File, status fuse.Status) {
	log.Printf("OP Open %s write=%v\n", name, flags&fuse.O_ANYWRITE != 0)
	node, status := cache.getNode(fs.ref, name)
	switch status {
	case fuse.OK:
	case fuse.ENOENT:
	default:
		return nil, status
	}
	rwPath, rwExists, err := fs.rwLayer.Exists(name)
	if err != nil {
		return nil, fuse.EIO
	}

	if flags&fuse.O_ANYWRITE != 0 || rwExists {
		log.Printf("OP Open write mode")
		if flags&fuse.O_ANYWRITE != 0 && fs.ro {
			// XXX(tsileo): is EROFS the right error code
			return nil, fuse.EROFS
		}
		// FIXME(tsileo): ensure the hash of the RWFile match the one in the node
		f, err := NewRWFile(fs, name, rwPath, flags, 0, node)
		log.Printf("RWFile result: %+v %+v\n", f, err)

		// There's no other file currently opened, add the file to the rw index
		fs.rwIndex(f)
		fs.mu.Lock()
		fs.openedFds++
		fs.mu.Unlock()
		return f, fuse.OK
	}
	if status == fuse.ENOENT {
		return nil, fuse.ENOENT
	}

	log.Printf("OPEN read mode\n")
	f, err := NewFile(fs, name, node)
	if err != nil {
		log.Printf("failed to open file for read-only: %v\n", err)
		return nil, fuse.EIO
	}

	fs.mu.Lock()
	fs.openedFds++
	fs.mu.Unlock()
	return f, fuse.OK
}

func (fs *FileSystem) Utimens(path string, a *time.Time, m *time.Time, context *fuse.Context) (code fuse.Status) {
	return fuse.EPERM
}

func (fs *FileSystem) Chmod(path string, mode uint32, context *fuse.Context) (code fuse.Status) {
	log.Printf("OP Chmod %s\n", path)
	return fuse.EPERM
}

func (fs *FileSystem) Chown(path string, uid uint32, gid uint32, context *fuse.Context) (code fuse.Status) {
	log.Printf("OP Chown %s\n", path)
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

func (fs *FileSystem) Mkdir(path string, mode uint32, _ *fuse.Context) (code fuse.Status) {
	log.Printf("OP Mkdir %s\n", path)

	if fs.ro {
		return fuse.EPERM
	}

	// Only continue if the file don't already exist
	_, status := cache.getNode(fs.ref, path)
	if status != fuse.ENOENT {
		// XXX(tsileo): check behavior here
		return status
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
	resp, err := kvs.Client().DoReq(context.TODO(), "PATCH", "/api/filetree/fs/fs/"+fs.ref+"/"+d, nil, bytes.NewReader(js))
	if err != nil || resp.StatusCode != 200 {
		return fuse.EIO
	}

	return fuse.OK
}

// Don't use os.Remove, it removes twice (unlink followed by rmdir).
func (fs *FileSystem) Unlink(name string, _ *fuse.Context) (code fuse.Status) {
	log.Printf("OP Unlink %s\n", name)
	if fs.ro {
		return fuse.EPERM
	}
	rwPath, rwExists, err := fs.rwLayer.Exists(name)
	if err != nil {
		return fuse.EIO
	}
	if rwExists {
		if err := fuse.ToStatus(syscall.Unlink(rwPath)); err != fuse.OK && err != fuse.ENOENT {
			// XXX(tsileo): what about ENOENT here, and no ENOENT on remote, should this be a special error?
			return err
		}
		// FIXME(tsileo): also delete the JSON file for file
	}

	// XXX(tsileo): should be inside the filetree client?
	resp, err := kvs.Client().DoReq(context.TODO(), "DELETE", "/api/filetree/fs/fs/"+fs.ref+"/"+name, nil, nil)
	if err == nil {
		switch resp.StatusCode {
		case 200, 204:
			return fuse.OK
		case 404:
			return fuse.ENOENT
		default:
			// TODO(tsileo): log the local error too
			return fuse.EIO
		}
	}

	return fuse.EIO
}

func (fs *FileSystem) Rmdir(name string, _ *fuse.Context) (code fuse.Status) {
	log.Printf("OP Rmdir %s\n", name)
	if fs.ro {
		return fuse.EPERM
	}

	node, status := cache.getNode(fs.ref, name)
	if status != fuse.OK {
		return status
	}

	if !node.IsDir() {
		return fuse.ENOTDIR
	}
	// Ensure the children check works
	if node.Children != nil && len(node.Children) > 0 {
		return fuse.Status(syscall.ENOTEMPTY)
	}

	resp, err := kvs.Client().DoReq(context.TODO(), "DELETE", "/api/filetree/fs/fs/"+fs.ref+"/"+name, nil, nil)
	if err != nil || resp.StatusCode != 204 {
		return fuse.EIO
	}

	return fuse.OK

	// FIXME(tsileo): handle RO mode
	// return fuse.EPERM
}

func (fs *FileSystem) Symlink(pointedTo string, linkName string, context *fuse.Context) (code fuse.Status) {
	return fuse.EPERM
}

func (fs *FileSystem) Rename(oldPath string, newPath string, _ *fuse.Context) (codee fuse.Status) {
	log.Printf("OP Rename %s %s\n", oldPath, newPath)
	if fs.ro {
		return fuse.EPERM
	}
	rwPath, rwExists, err := fs.rwLayer.Exists(oldPath)
	if err != nil {
		return fuse.EIO
	}
	if rwExists {
		return fuse.ToStatus(os.Rename(rwPath, filepath.Join(fs.rwLayer.path, newPath)))
	}

	node, status := cache.getNode(fs.ref, oldPath)
	if status != fuse.OK {
		return status
	}

	// First, we remove the old path
	resp, err := kvs.Client().DoReq(context.TODO(), "DELETE", "/api/filetree/fs/fs/"+fs.ref+"/"+oldPath, nil, nil)
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

	resp, err = kvs.Client().DoReq(context.TODO(), "PATCH", "/api/filetree/fs/fs/"+fs.ref+"/"+dest, h, nil)
	if err != nil || resp.StatusCode != 200 {
		return fuse.EIO
	}

	return fuse.OK
}

func (fs *FileSystem) Link(orig string, newName string, context *fuse.Context) (code fuse.Status) {
	// TODO(tsileo): to ENOSYS?
	return fuse.EPERM
}

func (fs *FileSystem) Access(name string, mode uint32, context *fuse.Context) (code fuse.Status) {
	log.Printf("OP Access %s\n", name)
	return fuse.OK
}

func (fs *FileSystem) Create(path string, flags uint32, mode uint32, context *fuse.Context) (fuseFile nodefs.File, code fuse.Status) {
	log.Printf("OP Create %s\n", path)
	// TODO(tsileo): check if it exists first?
	f, err := NewRWFile(fs, path, filepath.Join(fs.rwLayer.path, path), flags, mode, nil)
	log.Printf("rwfile=%+v %+v\n", f, err)
	fs.rwIndex(f)
	return f, fuse.ToStatus(err)

	// 	fullPath := filepath.Join(fs.rwLayer, path)
	// 	if _, err := os.Stat(filepath.Dir(fullPath)); err != nil {
	// 		if os.IsNotExist(err) {
	// 			if err := os.MkdirAll(filepath.Dir(fullPath), 0744); err != nil {
	// 				return nil, fuse.EIO
	// 			}
	// 		} else {
	// 			return nil, fuse.EIO
	// 		}
	// 	}
	// 	f, err := os.OpenFile(filepath.Join(fs.rwLayer, path), int(flags)|os.O_CREATE, os.FileMode(mode))
	// 	return nodefs.NewLoopbackFile(f), fuse.ToStatus(err)
	// return nil, fuse.EPERM
}

func (fs *FileSystem) GetXAttr(name string, attr string, context *fuse.Context) ([]byte, fuse.Status) {
	log.Printf("OP GetXAttr %s\n", name)
	node, err := cache.getNode(fs.ref, name)
	if err != fuse.OK {
		return nil, err
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
	log.Printf("OP SetXAttr %s\n", name)
	return fuse.EPERM
}

func (fs *FileSystem) ListXAttr(name string, context *fuse.Context) ([]string, fuse.Status) {
	log.Printf("OP ListXAttr %s\n", name)

	// TODO(tsileo): is this really needed?
	//rwPath, rwExists, err := fs.rwExists(name)
	//if err != nil {
	//return nil, fuse.EIO
	//}
	//if rwExists {
	//if list, err := xattr.List(filepath.Join(fs.rwLayer, name)); err == nil {
	//return list, fuse.OK
	//}
	//}

	node, status := cache.getNode(fs.ref, name)
	if status != fuse.OK {
		return nil, status
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
	return fuse.OK
}

type RWFile struct {
	nodefs.File
	node     *Node
	inode    *nodefs.Inode
	path     string
	oldPath  string // FIXME(tsileo): set it on remove
	filename string
	fs       *FileSystem
	flags    uint32
}

type RWFileMeta struct {
	Node *Node  `json:"node"`
	Path string `json:"path"`
}

// FIXME(tsileo): GetAttr for RWFile, that only take the size from the file, find a way to handle mtime

func NewRWFile(fs *FileSystem, path, fullPath string, flags, mode uint32, node *Node) (*RWFile, error) {
	_, filename := filepath.Split(path)
	fh, err := os.OpenFile(fullPath, int(flags), os.FileMode(mode))
	if os.IsNotExist(err) {
		fh, err = os.OpenFile(fullPath, int(flags)|os.O_CREATE, os.FileMode(mode)|0644)
		if err != nil {
			return nil, err
		}
		if node != nil {
			// Fetch the "raw node" (the raw json desc of the node that contains the list of data blobs)
			ctx := context.TODO()
			blob, err := cache.Get(ctx, node.Ref)
			if err != nil {
				return nil, fmt.Errorf("failed to fetch blob from cache: %v", err)
			}
			meta, err := rnode.NewNodeFromBlob(node.Ref, blob)
			if err != nil {
				return nil, fmt.Errorf("failed to build node from blob \"%s\": %v", blob, err)
			}

			tmtime := time.Unix(int64(node.Mtime()), 0)
			if err := os.Chtimes(fullPath, tmtime, tmtime); err != nil {
				return nil, err
			}
			if err := node.Copy(fh, meta); err != nil {
				return nil, err
			}
			if err := fh.Sync(); err != nil {
				return nil, err
			}
			if _, err := fh.Seek(0, os.SEEK_SET); err != nil {
				return nil, err
			}

			// Create a JSON-encoded meta file to help debugging in case of crashes
			m := &RWFileMeta{Node: node, Path: path}
			mf, err := os.Create(fullPath + ".json")
			if err != nil {
				return nil, err
			}
			defer mf.Close()
			if err := json.NewEncoder(mf).Encode(m); err != nil {
				return nil, err
			}
		}
	}
	if err != nil {
		return nil, err
	}
	lf := nodefs.NewLoopbackFile(fh)
	return &RWFile{
		fs:       fs,
		path:     path,
		node:     node,
		filename: filename,
		File:     lf,
		flags:    flags,
	}, nil
}

func (f *RWFile) Hash() string {
	rwf, err := os.Open(f.fs.rwLayer.Path(f.path))
	if err != nil {
		log.Fatal(err)
	}
	defer rwf.Close()

	h := blake2b.New256()
	if _, err := io.Copy(h, rwf); err != nil {
		panic(fmt.Errorf("failed to compute rwfile hash %+v: %v", f, err))
	}

	return fmt.Sprintf("%x", h.Sum(nil))
}

func (f *RWFile) Flush() fuse.Status {
	log.Printf("OP Flush %+s\n", f)
	if status := f.File.Flush(); status != fuse.OK {
		log.Printf("loopback file %+v failed with status %v\n", status)
		return status
	}

	// Sometimes the RWFile can still be a file opened in RO mode (if there's another opened file in RW mode)
	if f.flags&fuse.O_ANYWRITE != 0 && f.Hash() != f.node.Hash() {
		fullPath := f.fs.rwLayer.Path(f.path)
		rawNode, err := f.fs.up.PutAndRenameFile(fullPath, f.filename)
		if err != nil {
			log.Printf("failed to upload: %v", err)
			if os.IsNotExist(err) {
				// This means the file has been removed
				return f.File.Flush()
			}
			return fuse.EIO
		}
		js, err := json.Marshal(rawNode)
		if err != nil {
			return fuse.EIO
		}
		d := filepath.Dir(f.path)
		if d == "." {
			d = ""
		}

		resp, err := kvs.Client().DoReq(context.TODO(), "PATCH", "/api/filetree/fs/fs/"+f.fs.ref+"/"+d, nil, bytes.NewReader(js))
		if err != nil || resp.StatusCode != 200 {
			log.Printf("upload failed with resp %+v/%+v\n", resp, err)
			return fuse.EIO
		}

		f.fs.mu.Lock()
		f.fs.uploadedFiles++
		f.fs.mu.Unlock()

	}
	return fuse.OK
}

func (f *RWFile) SetInode(inode *nodefs.Inode) {
	log.Printf("RWFile.SetInode %+v\n", inode)
	f.inode = inode
	f.File.SetInode(inode)
}

func (f *RWFile) Release() {
	f.fs.mu.Lock()
	f.fs.openedFds--
	f.fs.mu.Unlock()
	var last bool
	if f.inode.AnyFile() == nil {
		last = true
	}
	log.Printf("OP Release %v (is_last_opened=%s)\n", f, last)

	// XXX(tsileo): We cannot returns an error here, but it's about deleting the cache, cannot see a better place
	// as we cannot detect the last flush
	if last {
		// If there's no other opened file, delete the rwfile cache
		rwPath := f.fs.rwLayer.Path(f.path)
		if err := os.Remove(rwPath); err != nil {
			panic(fmt.Errorf("failed to release rwfile: %v", err))
		}
		if err := os.Remove(rwPath + ".json"); err != nil {
			panic(fmt.Errorf("failed to release rwfile (json): %v", err))
		}
		// Remove from the rwIndex
		f.fs.removePathFromIndex(f.path)
	}

	f.File.Release()
}

func NewFile(fs *FileSystem, path string, node *Node) (*File, error) {
	log.Printf("NewFile %s\n", path)
	ctx := context.TODO()
	blob, err := cache.Get(ctx, node.Ref)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch blob from cache: %v", err)
	}
	meta, err := rnode.NewNodeFromBlob(node.Ref, blob)
	if err != nil {
		return nil, fmt.Errorf("failed to build node from blob \"%s\": %v", blob, err)
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
	log.Printf("File.SetInode %+v\n", inode)
	f.inode = inode
}

func (f *File) String() string {
	return fmt.Sprintf("File(%s, %s)", f.path, f.node.Ref)
}

func (f *File) Write(data []byte, off int64) (uint32, fuse.Status) {
	return 0, fuse.EROFS
}

func (f *File) Read(buf []byte, off int64) (res fuse.ReadResult, code fuse.Status) {
	log.Printf("OP Read %v\n", f)
	if _, err := f.r.ReadAt(buf, off); err != nil {
		return nil, fuse.EIO
	}
	return fuse.ReadResultData(buf), fuse.OK
}

func (f *File) Release() {
	f.fs.mu.Lock()
	f.fs.openedFds--
	f.fs.mu.Unlock()

	log.Printf("OP Release %v\n", f)
	f.r.Close()
}

func (f *File) Flush() fuse.Status {
	return fuse.OK
}

func (f *File) GetAttr(a *fuse.Attr) fuse.Status {
	log.Printf("OP File.GetAttr %v %+v\n", f, f.r)
	a.Mode = fuse.S_IFREG | 0644
	a.Size = uint64(f.node.Size)
	a.Ctime = f.node.Mtime()
	a.Mtime = f.node.Mtime()
	a.Owner = *owner
	return fuse.OK
}
