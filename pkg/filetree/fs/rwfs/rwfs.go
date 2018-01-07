package main

// import "a4.io/blobstash/pkg/filetree/fs"

import (
	"context"
	"crypto/sha1"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
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
	"github.com/mitchellh/go-ps"
	_ "github.com/pkg/xattr"

	bcache "a4.io/blobstash/pkg/cache"
	"a4.io/blobstash/pkg/client/blobstore"
	"a4.io/blobstash/pkg/client/clientutil"
	"a4.io/blobstash/pkg/client/kvstore"
	"a4.io/blobstash/pkg/config/pathutil"
	"a4.io/blobstash/pkg/ctxutil"
	rnode "a4.io/blobstash/pkg/filetree/filetreeutil/node"
	"a4.io/blobstash/pkg/filetree/reader/filereader"
	"a4.io/blobstash/pkg/filetree/writer"
)

// TODO(tsileo):
// - support file@<date> ; e.g.: file.txt@2017-5-4T21:30 ???
// - [/] `-snapshot` mode that lock to the current version, very efficient, can specify a snapshot version `-at`
// - [X] `-rw` mode that support mutation, will query BlobStah a lot (without `-live-update`)
// - [X] `-live-update` (or `-ro`?) mode to receive update via SSE (e.g. serve static over fuse, or Dropbox infinite like), allow caching and to invalidate cache on remote changes (need to be able to discard its own generated event via the hostname)
// - [ ] support data context (add timeout server-side), and merge the data context on unmount

const revisionHeader = "BlobStash-Filetree-FS-Revision"

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

	cache, err := newCache(cacheDir)
	if err != nil {
		fmt.Printf("failed to setup cache at %s: %v\n", cacheDir, err)
		os.Exit(1)
	}

	// Setup the clients for BlobStash
	host := os.Getenv("BLOBS_API_HOST")
	apiKey := os.Getenv("BLOBS_API_KEY")
	hostname, err := os.Hostname()
	if err != nil {
		fmt.Printf("failed to get hostname: %v\n", err)
		os.Exit(1)
	}

	clientUtil := clientutil.NewClientUtil(host,
		clientutil.WithAPIKey(apiKey),
		clientutil.WithHeader(ctxutil.FileTreeHostnameHeader, hostname),
		clientutil.WithHeader(ctxutil.NamespaceHeader, "rwfs-"+ref),
		clientutil.EnableMsgpack(),
		clientutil.EnableSnappyEncoding(),
	)

	bs := blobstore.New(clientUtil)
	kvs := kvstore.New(clientUtil)

	authOk, err := clientUtil.CheckAuth()
	if err != nil {
		fmt.Printf("failed to contact BlobStash: %v\n", err)
		os.Exit(1)
	}

	if !authOk {
		fmt.Printf("bad API key\n")
		os.Exit(1)
	}

	root, err := NewFileSystem(flag.Arg(1), flag.Arg(0), *debug, cache, cacheDir, bs, kvs, clientUtil)
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
				log.Printf("DEBUG:\n")
				fmt.Printf("[fs]\nfs.ref=%s fs.ops=%d fs.fds=%d fs.fds_rw=%d fs.eio=%d fs.uptime=%v\n", root.stats.Ref, root.stats.Ops, root.stats.OpenedFds, root.stats.RWOpenedFds, root.stats.Eios, time.Since(root.stats.startedAt))
				fmt.Printf("[cache]\ncache.hits=%d cache.hits_pct=%.2f cache.reqs=%d cache.added=%d cache.len=%d cache.size=%v\n", root.stats.CacheHits, float64(root.stats.CacheHits)*100.0/float64(root.stats.CacheReqs), root.stats.CacheReqs, root.stats.CacheAdded, root.cache.blobsCache.Len(), humanize.Bytes(uint64(root.cache.blobsCache.Size())))
				fmt.Printf("[blobstash]\nblobstash.uploaded_files=%d blobstash.uploaded_files_size=%s blobstash.remote_stat=%d\n", root.stats.UploadedFiles, humanize.Bytes(uint64(root.stats.UploadedFilesSize)), root.stats.RemoteStat)
				//fmt.Printf("[rw cache]rwlayer cached: %+v %+v\n", root.rwLayer.cache, root.rwLayer.index)
				fmt.Printf("[fds]\n")
				if len(root.stats.FDIndex) == 0 {
					fmt.Printf("no fds\n")
				}
				for _, fdi := range root.stats.FDIndex {
					ro := " [rw]"
					if !fdi.Writable {
						ro = " [ro]"
					}
					fmt.Printf("/%s%s [%d %s] [%s]\n", fdi.Path, ro, fdi.Pid, fdi.Executable, fdi.CreatedAt.Format(time.RFC3339))
				}
			}
		}()
	}

	nfs := pathfs.NewPathNodeFs(root, nil)

	conn := nodefs.NewFileSystemConnector(nfs.Root(), opts)

	mountOpts := fuse.MountOptions{
		// FIXME(tsileo): -o allow_other as a CLI flag
		// AllowOther: true,
		Options: []string{
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

	go state.Serve()
	log.Println("mounted successfully")

	// Start a loop to make a GC 10 minutes after the last modification (if any)
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		for tick := range ticker.C {
			root.stats.Lock()
			if !root.stats.lastMod.IsZero() && root.stats.updated {
				// FIXME(tsileo): make the delay configurable
				if tick.Sub(root.stats.lastMod) > 300*time.Second {
					root.stats.updated = false
					if err := root.GC(); err != nil {
						panic(err)
					}
				}
			}
			root.stats.Unlock()
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

	// FIXME(tsileo): URGENT trigger the GC before umount
	if root.stats.updated {
		if err := root.GC(); err != nil {
			log.Printf("failed to GC: %v\n", err)
		}
	}

	cache.Close()
	log.Println("unmounted")
	os.Exit(0)
}

type DebugVFS struct {
	fs    *FileSystem
	Path  string
	attr  *fuse.Attr
	index map[string]func(string, *fuse.Context) (nodefs.File, error)
}

func newVFSEntry(content func() interface{}) func(string, *fuse.Context) (nodefs.File, error) {
	return func(name string, _ *fuse.Context) (nodefs.File, error) {
		return nodefs.NewDataFile([]byte(fmt.Sprintf("%v", content()))), nil
	}
}

func newDebugVFS(fs *FileSystem) *DebugVFS {
	return &DebugVFS{
		fs:   fs,
		Path: ".fs",
		attr: &fuse.Attr{
			Mode:  fuse.S_IFDIR | 0755,
			Owner: *fuse.CurrentOwner(),
			Mtime: uint64(fs.stats.startedAt.Unix()),
			Ctime: uint64(fs.stats.startedAt.Unix()),
		},
		// FIXME(tsileo): more fine grained directory a la /proc on Linux,
		// also find a way to handle the /fs/new_snapshot file (write a comment in it and it snapshot+sync the stash
		// TODO(tsileo): find a way to list info about the opened FDs (one file per FD with the PID,
		// path...)
		index: map[string]func(string, *fuse.Context) (nodefs.File, error){
			"debug.json": func(name string, _ *fuse.Context) (nodefs.File, error) {
				js, err := json.Marshal(&fs.stats)
				if err != nil {
					return nil, err
				}
				return nodefs.NewDataFile(js), nil
			},
			"ref": newVFSEntry(func() interface{} {
				return fs.stats.Ref
			}),
			"eios": newVFSEntry(func() interface{} {
				return fs.stats.Eios
			}),
			"fds": newVFSEntry(func() interface{} {
				return fs.stats.OpenedFds
			}),
			"fds_rw": newVFSEntry(func() interface{} {
				return fs.stats.RWOpenedFds
			}),
			"ops": newVFSEntry(func() interface{} {
				return fs.stats.Ops
			}),
			"synced": newVFSEntry(func() interface{} {
				return !fs.stats.updated
			}),
			"last_mod": newVFSEntry(func() interface{} {
				return fs.stats.lastMod.Unix()
			}),
			"last_revision": newVFSEntry(func() interface{} {
				return fs.lastRevision
			}),
		},
	}
}

func (dvfs *DebugVFS) GetAttr(name string, fctx *fuse.Context) (*fuse.Attr, bool) {
	if name == dvfs.Path {
		return dvfs.attr, true
	}
	if _, ok := dvfs.index[filepath.Base(name)]; ok {
		return &fuse.Attr{
			Mode:  fuse.S_IFREG | 0444,
			Mtime: uint64(dvfs.fs.stats.startedAt.Unix()),
			Ctime: uint64(dvfs.fs.stats.startedAt.Unix()),
		}, true
	}
	return nil, false
}

func (dvfs *DebugVFS) Open(name string, fctx *fuse.Context) (nodefs.File, error) {
	if f, ok := dvfs.index[filepath.Base(name)]; ok {
		return f(name, fctx)
	}
	return nil, nil
}

func (dvfs *DebugVFS) OpenDir(name string, fctx *fuse.Context) ([]fuse.DirEntry, bool) {
	if name != dvfs.Path {
		return nil, false
	}
	output := []fuse.DirEntry{}
	for k, _ := range dvfs.index {
		output = append(output, fuse.DirEntry{
			Name: k,
			Mode: fuse.S_IFREG | 0444,
		})
	}
	return output, true
}

type rwLayer struct {
	fs *FileSystem

	path string

	cache map[string]*RWFileMeta
	index map[string][]*RWFileMeta

	mu sync.Mutex
}

func (rl *rwLayer) GetAttr(name string, context *fuse.Context) (*fuse.Attr, error) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	rwmeta, ok := rl.cache[name]
	if !ok {
		// Return nil as a ENOENT
		return nil, nil
	}

	var err error
	st := syscall.Stat_t{}
	if name == "" {
		// When GetAttr is called for the toplevel directory, we always want
		// to look through symlinks.
		err = syscall.Stat(rwmeta.loopbackPath, &st)
	} else {
		err = syscall.Lstat(rwmeta.loopbackPath, &st)
	}

	if err != nil {
		return nil, err
	}

	ctime, _ := st.Ctim.Unix()
	mtime, _ := st.Mtim.Unix()
	return &fuse.Attr{
		Mode:  fuse.S_IFREG | st.Mode,
		Size:  uint64(st.Size),
		Ctime: uint64(ctime),
		Mtime: uint64(mtime),
		Owner: *fuse.CurrentOwner(),
	}, nil
}

// OpenDir returns a map of the currently opened RW files for the given parent directory
func (rl *rwLayer) OpenDir(name string, context *fuse.Context) map[string]fuse.DirEntry {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	// Check the rw index by looking at the parent index
	output := map[string]fuse.DirEntry{}
	if idx, ok := rl.index[name]; ok {
		for _, meta := range idx {
			output[meta.filename] = fuse.DirEntry{Name: meta.filename, Mode: uint32(fuse.S_IFREG)}
		}
	}

	return output
}

func (rl *rwLayer) Chmod(path string, mode uint32, mtime int64, fctx *fuse.Context) error {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	// Now take a look a the remote node
	node, err := rl.fs.getNode(path)
	if err != nil {
		return nil
	}

	if node == nil {
		// FIXME(tsileo): RWFile.Chown should return fuse.EBADF in this case
		return fmt.Errorf("a remote note should exist")
	}

	resp, err := rl.fs.clientUtil.PatchMsgpack(
		rl.fs.remotePath(rl.fs.dir(path)),
		nil,
		clientutil.WithQueryArgs(map[string]string{
			"mtime":  strconv.Itoa(int(mtime)),
			"rename": strconv.FormatBool(true),
		}),
		clientutil.WithHeaders(map[string]string{
			"BlobStash-Filetree-Patch-Ref":  node.Ref,
			"BlobStash-Filetree-Patch-Mode": strconv.Itoa(int(mode)),
		}),
	)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if err := clientutil.ExpectStatusCode(resp, http.StatusOK); err != nil {
		return err
	}

	if rwmeta, ok := rl.cache[path]; ok {
		// Update the rw meta ref if needed
		newNode := &Node{}
		if err := clientutil.Unmarshal(resp, newNode); err != nil {
			return err
		}
		rwmeta.Node = newNode
		rwmeta.ref = newNode.Ref
	}

	rl.fs.updateLastRevision(resp)

	return nil
}

func (rl *rwLayer) Utimens(path string, m *time.Time, fctx *fuse.Context) error {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	// Fetch the remote node
	node, err := rl.fs.getNode(path)
	if err != nil {
		return err
	}

	if node == nil {
		// FIXME(tsileo): RWFile.Utimens should return fuse.EBADF in this case
		return fmt.Errorf("a remote node should exists for %s", path)
	}

	// We PATCH the node, just for updating its mtime
	resp, err := rl.fs.clientUtil.PatchMsgpack(
		rl.fs.remotePath(rl.fs.dir(path)),
		nil,
		clientutil.WithQueryArg("mtime", strconv.Itoa(int(m.Unix()))),
		clientutil.WithHeader("BlobStash-Filetree-Patch-Ref", node.Ref),
	)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if err := clientutil.ExpectStatusCode(resp, http.StatusOK); err != nil {
		return err
	}

	// Update the rw meta ref if needed
	if rwmeta, ok := rl.cache[path]; ok {
		newNode := &Node{}
		if err := clientutil.Unmarshal(resp, newNode); err != nil {
			return err
		}
		rwmeta.Node = newNode
		rwmeta.ref = newNode.Ref
	}

	rl.fs.updateLastRevision(resp)

	return nil
}

func (rl *rwLayer) Unlink(name string) error {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	rwmeta, ok := rl.cache[name]
	if !ok {
		return nil
	}

	// Don't use os.Remove, it removes twice (unlink followed by rmdir).
	if err := syscall.Unlink(rwmeta.loopbackPath + ".json"); err != nil {
		return err
	}

	return syscall.Unlink(rwmeta.loopbackPath)
}

func (rl *rwLayer) Release(meta *RWFileMeta) error {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	// If there's no other opened file, delete the rwfile cache
	if err := os.Remove(meta.loopbackPath); err != nil {
		return fmt.Errorf("failed to release rwfile: %v", err)
	}
	if err := os.Remove(meta.loopbackPath + ".json"); err != nil {
		return fmt.Errorf("failed to release rwfile (json): %v", err)
	}

	delete(rl.cache, meta.Path)

	// FIXME(tsileo): Remove from the rwIndex
	oldParentIdx := rl.index[meta.parent]
	newParentIdx := []*RWFileMeta{}

	for _, m := range oldParentIdx {
		if m.filename == meta.filename {
			continue
		}
		newParentIdx = append(newParentIdx, m)
	}

	rl.index[meta.parent] = newParentIdx

	return nil
}

func (rl *rwLayer) Rename(oldPath, newPath string) error {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	rwmeta, ok := rl.cache[oldPath]
	if !ok {
		// The file is not currently opened for writing
		return nil
	}

	oldParent := rwmeta.parent
	oldFilename := rwmeta.filename

	// Update the RW meta
	newParent, newFilename := filepath.Split(newPath)
	rwmeta.filename = newFilename
	rwmeta.parent = newParent
	rwmeta.Path = newPath

	// Re-create the JSON file for debug
	mf, err := os.Create(rwmeta.loopbackPath + ".json")
	if err != nil {
		return err
	}
	defer mf.Close()
	if err := json.NewEncoder(mf).Encode(rwmeta); err != nil {
		return err
	}

	// Update the caches/indexes
	delete(rl.cache, oldPath)
	rl.cache[newPath] = rwmeta

	// Remove index entry for the old path
	oldParentIdx := rl.index[oldParent]
	newOldParentIdx := []*RWFileMeta{}
	for _, m := range oldParentIdx {
		if m.filename == oldFilename {
			continue
		}
		newOldParentIdx = append(newOldParentIdx, m)
	}
	rl.index[oldParent] = newOldParentIdx

	if _, ok := rl.index[newParent]; !ok {
		rl.index[newParent] = []*RWFileMeta{}
	}

	// Setup the parent index for the new path
	newParentIndex := rl.index[newParent]
	newNewParentIdx := []*RWFileMeta{rwmeta}
	for _, m := range newParentIndex {
		if m.filename == rwmeta.filename {
			continue
		}
		newNewParentIdx = append(newNewParentIdx, m)
	}
	rl.index[newParent] = newParentIndex

	return nil
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
	Name       string                 `json:"name" msgpack:"n"`
	Ref        string                 `json:"ref" msgpack:"r"`
	Size       int                    `json:"size" msgpack:"s,omitempty"`
	Type       string                 `json:"type" msgpack:"t"`
	Children   []*Node                `json:"children" msgpack:"c,omitempty"`
	Metadata   map[string]interface{} `json:"metadata" msgpack:"md,omitempty"`
	ModTime    string                 `json:"mtime" msgpack:"mt"`
	ChangeTime string                 `json:"ctime" msgpack:"ct"`
	RawMode    int                    `json:"mode" msgpack:"mo"`
}

func (n *Node) Mode() uint32 {
	if n.RawMode > 0 {
		return uint32(n.RawMode)
	}
	if n.Type == rnode.File {
		return 0644
	} else {
		return 0755
	}
}

func (n *Node) Hash() string {
	if len(n.Metadata) == 0 {
		// It happens for empty file
		return "69217a3079908094e11121d042354a7c1f55b6482ca1a51e1b250dfd1ed0eef9"
	}
	return n.Metadata["blake2b-hash"].(string)
}

func (n *Node) IsDir() bool {
	return n.Type == rnode.Dir
}

func (n *Node) IsFile() bool {
	return n.Type == rnode.File
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

func (n *Node) Ctime() uint64 {
	if n.ChangeTime != "" {
		t, err := time.Parse(time.RFC3339, n.ChangeTime)
		if err != nil {
			panic(err)
		}
		return uint64(t.Unix())
	}
	return 0
}

func (n *Node) Copy(dst io.Writer, fs *FileSystem, meta *rnode.RawNode) error {
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

	fileReader := filereader.NewFile(ctx, fs.cache, meta, fcache)
	defer fileReader.Close()
	io.Copy(dst, fileReader)
	return nil
}

type Cache struct {
	fs *FileSystem
	mu sync.Mutex

	blobsCache *bcache.Cache

	nodeIndex       map[string]*Node
	negNodeIndex    map[string]struct{}
	remoteStatCache map[string]bool

	procCache map[int]string
}

func newCache(path string) (*Cache, error) {
	blobsCache, err := bcache.New(path, "blobs.cache", 256<<20) // 256MB on-disk LRU cache
	if err != nil {
		return nil, err
	}

	return &Cache{
		nodeIndex:       map[string]*Node{},
		negNodeIndex:    map[string]struct{}{},
		blobsCache:      blobsCache,
		procCache:       map[int]string{},
		remoteStatCache: map[string]bool{},
	}, nil
}

func (c *Cache) Close() error {
	return c.blobsCache.Close()
}

// Find a cache the executable name for the PID stored in the context
func (c *Cache) findProcExec(context *fuse.Context) string {
	c.mu.Lock()
	defer c.mu.Unlock()

	pid := int(context.Pid)
	if exec, ok := c.procCache[pid]; ok {
		return exec
	}
	p, err := ps.FindProcess(int(context.Pid))
	if err != nil {
		panic(err)
	}
	if p == nil {
		return "<unk>"
	}
	exec := p.Executable()
	c.procCache[pid] = exec
	return exec
}

func (c *Cache) Stat(ctx context.Context, hash string) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	locStat, err := c.blobsCache.Stat(hash)
	if err != nil {
		return false, err
	}
	if locStat {
		return true, nil
	}

	if val, ok := c.remoteStatCache[hash]; ok && val {
		return true, nil
	}

	stat, err := c.fs.bs.Stat(context.TODO(), hash)
	if err != nil {
		return false, err
	}

	if stat {
		c.remoteStatCache[hash] = stat
	}

	return stat, nil
}

// Get implements the BlobStore interface for filereader.File
func (c *Cache) Put(ctx context.Context, hash string, data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.fs.stats.Lock()
	c.fs.stats.CacheAdded++
	c.fs.stats.Unlock()
	// FIXME(tsileo): add a stat/exist check once the data contexes is implemented
	if err := c.fs.bs.Put(ctx, hash, data); err != nil {
		return err
	}

	if err := c.blobsCache.Add(hash, data); err != nil {
		return err
	}
	return nil
}

// Get implements the BlobStore interface for filereader.File
func (c *Cache) Get(ctx context.Context, hash string) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var err error
	cachedBlob, ok, err := c.blobsCache.Get(hash)
	if err != nil {
		return nil, fmt.Errorf("cache failed: %v", err)
	}
	var data []byte
	if ok {
		c.fs.stats.Lock()
		c.fs.stats.CacheHits++
		c.fs.stats.CacheReqs++
		c.fs.stats.Unlock()

		data = cachedBlob
	} else {
		c.fs.stats.Lock()
		c.fs.stats.CacheAdded++
		c.fs.stats.CacheReqs++
		c.fs.stats.Unlock()

		data, err = c.fs.bs.Get(ctx, hash)
		if err != nil {
			return nil, fmt.Errorf("failed to call blobstore: %v", err)
		}
		if err := c.blobsCache.Add(hash, data); err != nil {
			return nil, fmt.Errorf("failed to add to cache: %v", err)
		}
	}
	return data, nil
}

type FileSystem struct {
	ref   string
	debug bool
	ro    bool

	debugVFS *DebugVFS

	rwLayer *rwLayer
	cache   *Cache
	stats   *FSStats

	up         *writer.Uploader
	kvs        *kvstore.KvStore
	bs         *blobstore.BlobStore
	clientUtil *clientutil.ClientUtil

	lastRevision int64

	mu sync.Mutex
}

// FSStats holds some stats about the mounted FS
type FSStats struct {
	sync.Mutex
	startedAt time.Time
	lastMod   time.Time
	updated   bool

	Ref string `json:"fs_ref"`

	Ops         int64 `json:"fs_ops"`
	Eios        int64 `json:"fs_eios"`
	OpenedFds   int64 `json:"fs_fds"`
	RWOpenedFds int64 `json:"fs_rw_fds"`

	RemoteStat        int64 `json:"blobstash_remote_stat"`
	UploadedFiles     int64 `json:"blobstash_uploaded_files"`
	UploadedFilesSize int64 `json:"blobstash_uploaded_files_size"`

	CacheHits  int64 `json:"cache_hits"`
	CacheReqs  int64 `json:"cache_reqs"`
	CacheAdded int64 `json:"cache_added"`

	FDIndex map[string]*FDInfo `json:"fs_fds_infos"`
	// TODO(tsileo): store LastEIOs []*EIOInfo{Err, Path, OP}
}

// FDInfo holds informations about an opened file descriptor
type FDInfo struct {
	Pid        int
	Executable string // Executable name from the PID
	Path       string // Path of the opened file on the FS
	Writable   bool
	CreatedAt  time.Time
}

func NewFileSystem(ref, mountpoint string, debug bool, cache *Cache, cacheDir string, bs *blobstore.BlobStore, kvs *kvstore.KvStore, cu *clientutil.ClientUtil) (*FileSystem, error) {
	fs := &FileSystem{
		cache:      cache,
		bs:         bs,
		kvs:        kvs,
		clientUtil: cu,
		ref:        ref,
		debug:      debug,
		rwLayer:    nil,
		up:         writer.NewUploader(cache),
		stats: &FSStats{
			Ref:       ref,
			startedAt: time.Now(),
			FDIndex:   map[string]*FDInfo{},
		},
	}
	var err error
	fs.rwLayer, err = newRWLayer(filepath.Join(cacheDir, "rw_layer"))
	if err != nil {
		return nil, fmt.Errorf("failed to init rwLayer: %v", err)
	}

	fs.debugVFS = newDebugVFS(fs)

	fs.rwLayer.fs = fs
	cache.fs = fs

	return fs, nil
}

func (fs *FileSystem) GC() error {
	gcScript := fmt.Sprintf(`
local kvstore = require('kvstore')

local key = "_filetree:fs:%s"
local version = "%d"
local _, ref, _ = kvstore.get(key, version)

-- mark the actual KV entry
mark_kv(key, version)

-- mark the whole tree
mark_filetree_node(ref)
`, fs.ref, fs.lastRevision)

	log.Printf("starting GC...")
	// FIXME(tsileo): make the stash name configurable
	resp, err := fs.clientUtil.Post(
		fmt.Sprintf("/api/stash/rwfs-%s/_gc", fs.ref),
		[]byte(gcScript),
	)
	if err != nil {
		// FIXME(tsileo): find a better way to handle this?
		return err
	}
	defer resp.Body.Close()

	if err := clientutil.ExpectStatusCode(resp, http.StatusNoContent); err != nil {
		// FIXME(tsileo): find a better way to handle this?
		return err
	}

	log.Println("current snapshot saved")
	return nil
}

func (fs *FileSystem) updateLastRevision(resp *http.Response) {
	rev := resp.Header.Get(revisionHeader)
	if rev == "" {
		panic("missing FS revision in response")
	}
	var err error
	fs.lastRevision, err = strconv.ParseInt(rev, 10, 0)
	if err != nil {
		panic("invalid FS revision")
	}
}

func (fs *FileSystem) dir(path string) string {
	d := filepath.Dir(path)
	if d == "." {
		d = ""
	}
	return d
}

func (fs *FileSystem) remotePath(path string) string {
	return fmt.Sprintf("/api/filetree/fs/fs/%s/%s", fs.ref, path)
}

// getNode fetches the node at path from BlobStash, like a "remote stat".
func (fs *FileSystem) getNode(path string) (*Node, error) {
	fs.stats.Lock()
	fs.stats.RemoteStat++
	fs.stats.Unlock()

	resp, err := fs.clientUtil.Get(fs.remotePath(path))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if err := clientutil.ExpectStatusCode(resp, http.StatusOK); err != nil {
		if err.IsNotFound() {
			// Return nil as ENOENT
			return nil, nil
		}
		return nil, err
	}

	node := &Node{}
	if err := clientutil.Unmarshal(resp, node); err != nil {
		return nil, err
	}

	return node, nil
}

func (fs *FileSystem) String() string {
	return fmt.Sprintf("BlobFS(%s)", fs.ref)
}

func (fs *FileSystem) SetDebug(debug bool) {
	//fs.debug = debug
}

func (*FileSystem) StatFs(name string) *fuse.StatfsOut {
	return &fuse.StatfsOut{}
}

func (*FileSystem) OnMount(*pathfs.PathNodeFs) {}

func (*FileSystem) OnUnmount() {}

func (fs *FileSystem) logEIO(err error) {
	panic(err)
	log.Printf("EIO error: %+v\n", err)
	fs.stats.Lock()
	defer fs.stats.Unlock()
	fs.stats.Eios++
}

func (fs *FileSystem) logOP(opCode, path string, write bool, fctx *fuse.Context) {
	fs.stats.Lock()
	fs.stats.Ops++
	if write {
		fs.stats.updated = true
		fs.stats.lastMod = time.Now()
	}
	fs.stats.Unlock()
	if fs.debug {
		exec := fs.cache.findProcExec(fctx)
		log.Printf("OP %s path=/%s pid=%d %s\n", opCode, path, fctx.Pid, exec)
	}
}

func (fs *FileSystem) GetAttr(name string, fctx *fuse.Context) (*fuse.Attr, fuse.Status) {
	fs.logOP("Stat", name, false, fctx)
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// First, check if a debug/fake data file is requested
	if a, ok := fs.debugVFS.GetAttr(name, fctx); ok {
		return a, fuse.OK
	}

	// Check if the requested path is a file already opened for writing
	rwAttr, err := fs.rwLayer.GetAttr(name, fctx)
	if err != nil {
		fs.logEIO(err)
		return nil, fuse.EIO
	}

	// The file is already opened for writing, return the stat result
	if rwAttr != nil {
		return rwAttr, fuse.OK
	}

	// The is not already opened for writing, contact BlobStash
	node, err := fs.getNode(name)
	if err != nil {
		fs.logEIO(err)
		return nil, fuse.EIO
	}

	// Quick hack, this happen when an empty root was just initialized and not saved yet
	if name == "" && node == nil {
		return &fuse.Attr{
			Mode:  fuse.S_IFDIR | 0755,
			Owner: *fuse.CurrentOwner(),
			Mtime: uint64(fs.stats.startedAt.Unix()),
			Ctime: uint64(fs.stats.startedAt.Unix()),
		}, fuse.OK
	}

	if node == nil {
		return nil, fuse.ENOENT
	}

	if node.IsDir() {
		return &fuse.Attr{
			Mode:  fuse.S_IFDIR | node.Mode(),
			Ctime: node.Ctime(),
			Mtime: node.Mtime(),
			Owner: *fuse.CurrentOwner(),
		}, fuse.OK
	}

	// The node is a file
	return &fuse.Attr{
		Mode:  fuse.S_IFREG | node.Mode(),
		Size:  uint64(node.Size),
		Ctime: node.Ctime(),
		Mtime: node.Mtime(),
		Owner: *fuse.CurrentOwner(),
	}, fuse.OK
}

func (fs *FileSystem) OpenDir(name string, fctx *fuse.Context) ([]fuse.DirEntry, fuse.Status) {
	fs.logOP("OpenDir", name, false, fctx)

	debugDir, ok := fs.debugVFS.OpenDir(name, fctx)
	if ok {
		return debugDir, fuse.OK
	}

	var err error
	// The real directory is the 1st layer, if a file exists locally as a file it will show up instead of the remote version
	//if !fs.ro {
	index := fs.rwLayer.OpenDir(name, fctx)
	if index == nil {
		index = map[string]fuse.DirEntry{}
	}

	// Quick hack to add the magic "/fs" directory
	if name == "" {
		// The root is requested, we want to show the ".fs" dir
		index[fs.debugVFS.Path] = fuse.DirEntry{
			Name: fs.debugVFS.Path,
			Mode: uint32(fuse.S_IFDIR | 0755),
		}
	}

	// Now take a look a the remote node
	node, err := fs.getNode(name)
	if err != nil {
		fs.logEIO(err)
		return nil, fuse.EIO
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
				mode := fuse.S_IFDIR | int(child.Mode())
				if child.IsFile() {
					mode = fuse.S_IFREG | int(child.Mode())
				}
				index[child.Name] = fuse.DirEntry{
					Name: child.Name,
					Mode: uint32(mode),
				}
			}
		}
	}
	output := []fuse.DirEntry{}
	for _, dirEntry := range index {
		output = append(output, dirEntry)
	}

	return output, fuse.OK
}

func (fs *FileSystem) Open(name string, flags uint32, fctx *fuse.Context) (nodefs.File, fuse.Status) {
	fs.logOP("Open", name, false, fctx)
	fs.mu.Lock()
	defer fs.mu.Unlock()

	debugFile, err := fs.debugVFS.Open(name, fctx)
	if err != nil {
		fs.logEIO(err)
		return nil, fuse.EIO
	}
	if debugFile != nil {
		return debugFile, fuse.OK
	}

	node, err := fs.getNode(name)
	if err != nil {
		fs.logEIO(fmt.Errorf("failed to get node: %v", err))
		return nil, fuse.EIO
	}

	fs.rwLayer.mu.Lock()
	_, rwExists := fs.rwLayer.cache[name]
	fs.rwLayer.mu.Unlock()

	if flags&fuse.O_ANYWRITE != 0 || rwExists {
		if flags&fuse.O_ANYWRITE != 0 && fs.ro {
			// XXX(tsileo): is EROFS the right error code
			return nil, fuse.EROFS
		}
		f, err := NewRWFile(context.TODO(), fctx, fs, name, flags, 0644, node)
		if err != nil {
			fs.logEIO(fmt.Errorf("failed to open RW file: %v", err))
			return nil, fuse.EIO
		}

		return f, fuse.OK
	}
	if node == nil {
		return nil, fuse.ENOENT
	}

	f, err := NewFile(fctx, fs, name, node)
	if err != nil {
		fs.logEIO(fmt.Errorf("failed to open file for read-only: %v\n", err))
		return nil, fuse.EIO
	}

	return f, fuse.OK
}

func (fs *FileSystem) Utimens(path string, a *time.Time, m *time.Time, fctx *fuse.Context) fuse.Status {
	fs.logOP("Utimens", path, true, fctx)

	if err := fs.rwLayer.Utimens(path, m, fctx); err != nil {
		fs.logEIO(err)
		return fuse.EIO
	}

	return fuse.OK
}

func (fs *FileSystem) Chmod(path string, mode uint32, fctx *fuse.Context) fuse.Status {
	fs.logOP("Chmod", path, true, fctx)

	mtime := time.Now().Unix()

	if err := fs.rwLayer.Chmod(path, mode, mtime, fctx); err != nil {
		fs.logEIO(err)
		return fuse.EIO
	}

	return fuse.OK
}

func (fs *FileSystem) Chown(path string, uid uint32, gid uint32, fctx *fuse.Context) fuse.Status {
	fs.logOP("Chown", path, false, fctx)
	return fuse.ENOSYS
}

func (fs *FileSystem) Truncate(path string, offset uint64, fctx *fuse.Context) fuse.Status {
	fs.logOP("Truncate", path, false, fctx)
	return fuse.ENOSYS
}

func (fs *FileSystem) Readlink(name string, fctx *fuse.Context) (string, fuse.Status) {
	fs.logOP("Readlink", name, false, fctx)
	return "", fuse.ENOSYS
}

func (fs *FileSystem) Mknod(name string, mode uint32, dev uint32, fctx *fuse.Context) fuse.Status {
	fs.logOP("Mknod", name, false, fctx)
	return fuse.ENOSYS
}

func (fs *FileSystem) Mkdir(path string, mode uint32, fctx *fuse.Context) fuse.Status {
	fs.logOP("Mkdir", path, true, fctx)

	if fs.ro {
		return fuse.EPERM
	}

	// Only continue if the node don't already exist
	remoteNode, err := fs.getNode(path)
	if err != nil {
		fs.logEIO(err)
		return fuse.EIO
	}

	if remoteNode != nil {
		// FIXME(tsileo): what is the right behavior if the dir already exists? short circuit the call for now.
		return fuse.OK
	}

	mtime := time.Now().Unix()

	node := &rnode.RawNode{
		Version: rnode.V1,
		Type:    rnode.Dir,
		Name:    filepath.Base(path),
		ModTime: mtime,
	}

	resp, err := fs.clientUtil.PatchMsgpack(
		fs.remotePath(fs.dir(path)),
		node,
		clientutil.WithQueryArg("mtime", strconv.FormatInt(mtime, 10)),
	)
	if err != nil {
		fs.logEIO(err)
		return fuse.EIO
	}

	defer resp.Body.Close()

	if err := clientutil.ExpectStatusCode(resp, http.StatusOK); err != nil {
		fs.logEIO(err)
		return fuse.EIO
	}

	fs.updateLastRevision(resp)

	return fuse.OK
}

func (fs *FileSystem) Unlink(name string, fctx *fuse.Context) fuse.Status {
	fs.logOP("Unlink", name, true, fctx)
	// FIXME(tsileo): lock the fs here
	if fs.ro {
		return fuse.EPERM
	}

	if err := fs.rwLayer.Unlink(name); err != nil {
		fs.logEIO(err)
		return fuse.EIO
	}

	mtime := time.Now().Unix()
	resp, err := fs.clientUtil.Delete(
		fs.remotePath(name),
		clientutil.WithQueryArg("mtime", strconv.FormatInt(mtime, 10)),
	)
	if err != nil {
		fs.logEIO(err)
		return fuse.EIO
	}
	defer resp.Body.Close()

	if err := clientutil.ExpectStatusCode(resp, http.StatusNoContent); err != nil {
		if err.IsNotFound() {
			return fuse.ENOENT
		}

		fs.logEIO(err)
		return fuse.EIO
	}

	fs.updateLastRevision(resp)

	return fuse.OK
}

func (fs *FileSystem) Rmdir(name string, fctx *fuse.Context) fuse.Status {
	fs.logOP("Rmdir", name, true, fctx)
	if fs.ro {
		return fuse.EPERM
	}

	node, err := fs.getNode(name)
	if err != nil {
		fs.logEIO(err)
		return fuse.EIO
	}

	if !node.IsDir() {
		return fuse.ENOTDIR
	}
	// Ensure the children check works
	if node.Children != nil && len(node.Children) > 0 {
		return fuse.Status(syscall.ENOTEMPTY)
	}

	mtime := time.Now().Unix()
	resp, err := fs.clientUtil.Delete(
		fs.remotePath(name),
		clientutil.WithQueryArg("mtime", strconv.FormatInt(mtime, 10)),
	)
	if err != nil {
		fs.logEIO(err)
		return fuse.EIO
	}
	defer resp.Body.Close()

	if err := clientutil.ExpectStatusCode(resp, http.StatusNoContent); err != nil {
		fs.logEIO(err)
		return fuse.EIO
	}

	fs.updateLastRevision(resp)

	return fuse.OK
}

func (fs *FileSystem) Symlink(pointedTo string, linkName string, fctx *fuse.Context) fuse.Status {
	fs.logOP("Symlink", fmt.Sprintf("pointed_to=%s link_name=%s", pointedTo, linkName), false, fctx)
	return fuse.EPERM
}

func (fs *FileSystem) Rename(oldPath string, newPath string, fctx *fuse.Context) fuse.Status {
	fs.logOP("Rename", fmt.Sprintf("%s new=%s", oldPath, newPath), true, fctx)

	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.ro {
		return fuse.EPERM
	}

	if err := fs.rwLayer.Rename(oldPath, newPath); err != nil {
		fs.logEIO(err)
		return fuse.EIO
	}

	mtime := time.Now().Unix()

	node, err := fs.getNode(oldPath)
	if err != nil {
		fs.logEIO(err)
		return fuse.EIO
	}

	if node == nil {
		return fuse.ENOENT
	}

	// XXX the delete is in two phase: delete the original node and re-add it to its new parent

	// First, we remove the old path
	resp, err := fs.clientUtil.Delete(
		fs.remotePath(oldPath),
		clientutil.WithQueryArg("mtime", strconv.FormatInt(mtime, 10)),
	)
	if err != nil {
		fs.logEIO(err)
		return fuse.EIO
	}
	defer resp.Body.Close()

	if err := clientutil.ExpectStatusCode(resp, http.StatusNoContent); err != nil {
		fs.logEIO(err)
		return fuse.EIO
	}

	// Next, we re-add it to its dest
	resp, err = fs.clientUtil.PatchMsgpack(
		fs.remotePath(fs.dir(newPath)),
		nil,
		clientutil.WithHeaders(map[string]string{
			"BlobStash-Filetree-Patch-Ref":  node.Ref,
			"BlobStash-Filetree-Patch-Name": filepath.Base(newPath),
		}),
		clientutil.WithQueryArgs(map[string]string{
			// FIXME(tsileo): s/rename/change/ ?
			"rename": strconv.FormatBool(true),
			"mtime":  strconv.Itoa(int(mtime)),
		}),
	)
	if err != nil {
		fs.logEIO(err)
		return fuse.EIO
	}
	defer resp.Body.Close()

	if err := clientutil.ExpectStatusCode(resp, http.StatusOK); err != nil {
		fs.logEIO(err)
		return fuse.EIO
	}

	fs.updateLastRevision(resp)

	return fuse.OK
}

func (fs *FileSystem) Link(orig string, newName string, fctx *fuse.Context) fuse.Status {
	fs.logOP("Link", fmt.Sprintf("orig=%s new_name=%s", orig, newName), false, fctx)
	return fuse.ENOSYS
}

func (fs *FileSystem) Access(name string, mode uint32, fctx *fuse.Context) fuse.Status {
	fs.logOP("Access", name, false, fctx)
	// FIXME(tsileo): better impl
	return fuse.OK
}

func (fs *FileSystem) Create(path string, flags uint32, mode uint32, fctx *fuse.Context) (nodefs.File, fuse.Status) {
	fs.logOP("Create", path, true, fctx)

	mtime := time.Now().Unix()

	// XXX(tsileo): this should be in the rw layer
	// Only continue if the node don't already exist
	remoteNode, err := fs.getNode(path)
	if err != nil {
		fs.logEIO(err)
		return nil, fuse.EIO
	}
	if remoteNode != nil {
		fs.logEIO(fmt.Errorf("a node already exist at %s", path))
		return nil, fuse.EIO
	}

	node := &rnode.RawNode{
		Type:    rnode.File,
		Name:    filepath.Base(path),
		Version: rnode.V1,
		ModTime: mtime,
		Mode:    mode,
	}

	resp, err := fs.clientUtil.PatchMsgpack(
		fs.remotePath(fs.dir(path)),
		node,
		clientutil.WithQueryArg("mtime", strconv.FormatInt(mtime, 10)),
	)
	if err != nil {
		fs.logEIO(err)
		return nil, fuse.EIO
	}
	defer resp.Body.Close()

	if err := clientutil.ExpectStatusCode(resp, http.StatusOK); err != nil {
		fs.logEIO(err)
		return nil, fuse.EIO
	}

	newNode := &Node{}
	if err := clientutil.Unmarshal(resp, newNode); err != nil {
		fs.logEIO(err)
		return nil, fuse.EIO
	}

	fs.updateLastRevision(resp)

	f, err := NewRWFile(context.TODO(), fctx, fs, path, flags, mode, newNode)
	if err != nil {
		fs.logEIO(err)
		return nil, fuse.EIO
	}

	return f, fuse.OK
}

func (fs *FileSystem) GetXAttr(name string, attr string, fctx *fuse.Context) ([]byte, fuse.Status) {
	fs.logOP("GetXAttr", name, false, fctx)
	node, err := fs.getNode(name)
	if err != nil {
		fs.logEIO(err)
		return nil, fuse.EIO
	}

	if node == nil {
		return nil, fuse.ENOATTR // FIXME(tsileo): better error?
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
	fs.logOP("SetXAttr", name, false, context)
	return fuse.EPERM
}

func (fs *FileSystem) ListXAttr(name string, fctx *fuse.Context) ([]string, fuse.Status) {
	fs.logOP("ListXAttr", name, false, fctx)

	// FIXME(tsileo): what to do about Xattr for opened rwfile?

	node, err := fs.getNode(name)
	if err != nil {
		fs.logEIO(err)
		return nil, fuse.EIO
	}

	if node == nil {
		return nil, fuse.ENOENT // FIXME(tsileo): is this the right code?
	}

	out := []string{"node.ref"}
	if node.Metadata != nil {
		for k, _ := range node.Metadata {
			out = append(out, fmt.Sprintf("node.metadata.%s", k))
		}
	}
	return out, fuse.OK
}

func (fs *FileSystem) RemoveXAttr(name string, attr string, fctx *fuse.Context) fuse.Status {
	fs.logOP("RemoveXAttr", name, false, fctx)
	return fuse.OK
}

type RWFile struct {
	nodefs.File
	meta  *RWFileMeta
	node  *Node
	inode *nodefs.Inode
	fs    *FileSystem
	flags uint32
	fctx  *fuse.Context
	fid   string
}

type RWFileMeta struct {
	Node *Node  `json:"node"`
	Path string `json:"path"`

	ref          string
	loopbackPath string
	filename     string
	parent       string
}

// FIXME(tsileo): GetAttr for RWFile, that only take the size from the file, find a way to handle mtime
func newRWFileMeta(fs *FileSystem, node *Node, path string) *RWFileMeta {
	parent, filename := filepath.Split(path)

	// Generate a random filename for the loopback file
	rnd := fmt.Sprintf("%s:%d", path, time.Now().UnixNano())
	loopbackFilename := fmt.Sprintf("%x", sha1.Sum([]byte(rnd)))

	return &RWFileMeta{
		Node:         node, // Store the orignal node (for debug purpose only)
		Path:         path, // The path (for debug purpose)
		ref:          node.Ref,
		loopbackPath: filepath.Join(fs.rwLayer.path, loopbackFilename),
		filename:     filename,
		parent:       parent,
	}
}

func NewRWFile(ctx context.Context, fctx *fuse.Context, fs *FileSystem, path string, flags, mode uint32, node *Node) (*RWFile, error) {
	fs.rwLayer.mu.Lock()
	defer fs.rwLayer.mu.Unlock()

	var meta *RWFileMeta
	var lflags int
	var lmode os.FileMode
	var initialLoad bool

	if existingMeta, ok := fs.rwLayer.cache[path]; ok {
		meta = existingMeta
		lflags = int(flags)
		lmode = os.FileMode(mode)
	} else {
		// This is the first fd for this node
		meta = newRWFileMeta(fs, node, path)
		fs.rwLayer.cache[path] = meta
		if i, ok := fs.rwLayer.index[meta.parent]; ok {
			i = append(i, meta)
		} else {
			fs.rwLayer.index[meta.parent] = []*RWFileMeta{meta}
		}

		lflags = int(flags) | os.O_CREATE
		lmode = os.FileMode(mode) | 0644 // XXX(tsileo): is this needed?
		initialLoad = true

		// Create a JSON file for debug
		mf, err := os.Create(meta.loopbackPath + ".json")
		if err != nil {
			return nil, err
		}
		defer mf.Close()
		if err := json.NewEncoder(mf).Encode(meta); err != nil {
			return nil, err
		}
	}

	fh, err := os.OpenFile(meta.loopbackPath, lflags, lmode)
	if err != nil {
		return nil, err
	}
	if initialLoad && node != nil {
		// Fetch the "raw node" (the raw json desc of the node that contains the list of data blobs)
		blob, err := fs.cache.Get(ctx, node.Ref)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch blob from cache: %v", err)
		}
		nodeMeta, err := rnode.NewNodeFromBlob(node.Ref, blob)
		if err != nil {
			return nil, fmt.Errorf("failed to build node from blob \"%s\": %v", blob, err)
		}

		tmtime := time.Unix(int64(node.Mtime()), 0)
		if err := os.Chtimes(meta.loopbackPath, tmtime, tmtime); err != nil {
			return nil, err
		}
		if err := os.Chmod(meta.loopbackPath, os.FileMode(mode)); err != nil {
			return nil, err
		}

		if err := node.Copy(fh, fs, nodeMeta); err != nil {
			return nil, err
		}
		if err := fh.Sync(); err != nil {
			return nil, err
		}
		if _, err := fh.Seek(0, os.SEEK_SET); err != nil {
			return nil, err
		}
	}

	// Create the loopback file (provided by go-fuse)
	lf := nodefs.NewLoopbackFile(fh)

	rnd := fmt.Sprintf("%s:%d:%d", path, fctx.Pid, time.Now().UnixNano())
	fid := fmt.Sprintf("%x", sha1.Sum([]byte(rnd)))[:6]

	fs.stats.Lock()
	fs.stats.OpenedFds++
	fs.stats.RWOpenedFds++
	fs.stats.FDIndex[fid] = &FDInfo{
		Path:       path,
		Pid:        int(fctx.Pid),
		Executable: fs.cache.findProcExec(fctx),
		Writable:   true,
		CreatedAt:  time.Now(),
	}
	fs.stats.Unlock()

	return &RWFile{
		meta:  meta,
		fs:    fs,
		node:  node,
		File:  lf,
		flags: flags,
		fctx:  fctx,
		fid:   fid,
	}, nil
}

func (f *RWFile) logOP(op, extra string, write bool) {
	if extra != "" {
		extra = " " + extra
	}
	f.fs.logOP(fmt.Sprintf("RWFile.%s", op), f.meta.Path+extra, write, f.fctx)
}

func (f *RWFile) Hash() string {
	rwf, err := os.Open(f.meta.loopbackPath)
	if err != nil {
		log.Fatal(err)
	}
	defer rwf.Close()

	h := blake2b.New256()
	if _, err := io.Copy(h, rwf); err != nil {
		// TODO(tsileo): return an error?
		panic(fmt.Errorf("failed to compute rwfile hash %+v: %v", f, err))
	}

	return fmt.Sprintf("%x", h.Sum(nil))
}

func (f *RWFile) Read(dest []byte, off int64) (fuse.ReadResult, fuse.Status) {
	f.logOP("Read", fmt.Sprintf("size=%d offset=%d", len(dest), off), false)

	return f.File.Read(dest, off)
}

func (f *RWFile) Write(data []byte, off int64) (uint32, fuse.Status) {
	f.logOP("Write", fmt.Sprintf("size=%d offset=%d", len(data), off), true)
	return f.File.Write(data, off)
}

func (f *RWFile) Chmod(perms uint32) fuse.Status {
	f.logOP("Chmod", "", true)

	mtime := time.Now().Unix()

	if err := f.fs.rwLayer.Chmod(f.meta.Path, perms, mtime, f.fctx); err != nil {
		f.fs.logEIO(err)
		return fuse.EIO
	}

	return f.File.Chmod(perms)
}

func (f *RWFile) Utimens(atime *time.Time, mtime *time.Time) fuse.Status {
	f.logOP("Utimens", "", true)

	if err := f.fs.rwLayer.Utimens(f.meta.Path, mtime, f.fctx); err != nil {
		f.fs.logEIO(err)
		return fuse.EIO
	}

	return f.File.Utimens(atime, mtime)
}

func (f *RWFile) GetAttr(out *fuse.Attr) fuse.Status {
	f.logOP("Stat", "", false)
	attr, err := f.fs.rwLayer.GetAttr(f.meta.Path, f.fctx)
	if err != nil {
		f.fs.logEIO(err)
		return fuse.EIO
	}

	// if attr is nil, it means the file is already closed
	if attr == nil {
		return fuse.EBADF
	}

	*out = *attr

	return fuse.OK
}

func (f *RWFile) Chown(uid uint32, gid uint32) fuse.Status {
	f.logOP("Chown", "", false)
	return fuse.ENOSYS
}

func (f *RWFile) Allocate(off uint64, size uint64, mode uint32) fuse.Status {
	f.logOP("Allocate", "", false)
	return fuse.ENOSYS
}

func (f *RWFile) Flush() fuse.Status {
	f.logOP("Flush", "", true)

	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()

	if status := f.File.Flush(); status != fuse.OK {
		if status == fuse.EIO {
			f.fs.logEIO(fmt.Errorf("loopback file %+v failed with status %v", status))
		}
		return status
	}

	// Sometimes the RWFile can still be a file opened in RO mode (if there's another opened file in RW mode)
	if f.flags&fuse.O_ANYWRITE != 0 && (f.node == nil || f.node.Hash() != f.Hash()) {
		rawNode, err := f.fs.up.PutFileRename(f.meta.loopbackPath, f.meta.filename, true)
		if err != nil {
			if os.IsNotExist(err) {
				// This means the file has been removed
				return f.File.Flush()
			}
			f.fs.logEIO(fmt.Errorf("failed to upload: %v", err))
			return fuse.EIO
		}

		resp, err := f.fs.clientUtil.PatchMsgpack(
			f.fs.remotePath(f.fs.dir(f.meta.Path)),
			rawNode,
			clientutil.WithQueryArgs(map[string]string{
				"mtime": strconv.Itoa(int(rawNode.ModTime)),
			}))
		if err != nil {
			f.fs.logEIO(fmt.Errorf("upload failed: %v", err))
			return fuse.EIO
		}
		defer resp.Body.Close()

		if err := clientutil.ExpectStatusCode(resp, http.StatusOK); err != nil {
			f.fs.logEIO(err)
			return fuse.EIO
		}

		// FIXME(tsileo): save it in *FileSystem, we'll need it when GCing the stash
		// Also do it everywhere it's needed
		// rev := resp.Header.Get("BlobStash-Filetree-FS-Revision")

		// Update the rw meta ref
		newNode := &Node{}
		if err := clientutil.Unmarshal(resp, newNode); err != nil {
			f.fs.logEIO(err)
			return fuse.EIO
		}

		f.fs.updateLastRevision(resp)

		f.meta.ref = newNode.Ref

		f.fs.stats.Lock()
		f.fs.stats.UploadedFiles++
		f.fs.stats.UploadedFilesSize += int64(rawNode.Size)
		f.fs.stats.Unlock()
	}
	return fuse.OK
}

func (f *RWFile) SetInode(inode *nodefs.Inode) {
	f.inode = inode
	f.File.SetInode(inode)
}

func (f *RWFile) Release() {
	f.logOP("Release", "", false)
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()

	f.fs.stats.Lock()
	f.fs.stats.OpenedFds--
	f.fs.stats.RWOpenedFds--
	delete(f.fs.stats.FDIndex, f.fid)
	f.fs.stats.Unlock()

	var last bool
	if f.inode.AnyFile() == nil {
		last = true
	}

	// XXX(tsileo): We cannot returns an error here, but it's about deleting the cache, cannot see a better place
	// as we cannot detect the last flush
	//if last {
	if last {
		if err := f.fs.rwLayer.Release(f.meta); err != nil {
			f.fs.logEIO(err)
			panic(err)
		}
	}

	f.File.Release()
}

func NewFile(fctx *fuse.Context, fs *FileSystem, path string, node *Node) (*File, error) {
	ctx := context.TODO()
	blob, err := fs.cache.Get(ctx, node.Ref)
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
	r := filereader.NewFile(ctx, fs.cache, meta, fcache)

	rnd := fmt.Sprintf("%s:%d:%d", path, fctx.Pid, time.Now().UnixNano())
	fid := fmt.Sprintf("%x", sha1.Sum([]byte(rnd)))[:6]

	fs.stats.Lock()
	fs.stats.OpenedFds++
	fs.stats.FDIndex[fid] = &FDInfo{
		Path:       path,
		Pid:        int(fctx.Pid),
		Executable: fs.cache.findProcExec(fctx),
		Writable:   false,
		CreatedAt:  time.Now(),
	}
	fs.stats.Unlock()

	return &File{
		node: node,
		fs:   fs,
		path: path,
		r:    r,
		fctx: fctx,
		fid:  fid,
	}, nil
}

type File struct {
	nodefs.File
	inode *nodefs.Inode

	path string
	node *Node

	r *filereader.File

	fid  string
	fctx *fuse.Context
	fs   *FileSystem
}

func (f *File) logOP(op, extra string) {
	if extra != "" {
		extra = " " + extra
	}
	f.fs.logOP(fmt.Sprintf("File.%s [ro] %s", op, f.fid), f.path+extra, false, f.fctx)
}

func (f *File) SetInode(inode *nodefs.Inode) {
	f.inode = inode
}

func (f *File) String() string {
	return fmt.Sprintf("File(%s, %s)", f.path, f.node.Ref)
}

func (f *File) Write(data []byte, off int64) (uint32, fuse.Status) {
	f.logOP("Write", fmt.Sprintf("size=%d offset=%d", len(data), off))
	return 0, fuse.EROFS
}

func (f *File) Read(buf []byte, off int64) (res fuse.ReadResult, code fuse.Status) {
	f.logOP("Read", fmt.Sprintf("size=%d offset=%d", len(buf), off))

	if _, err := f.r.ReadAt(buf, off); err != nil {
		f.fs.logEIO(err)
		return nil, fuse.EIO
	}
	return fuse.ReadResultData(buf), fuse.OK
}

func (f *File) Release() {
	f.logOP("Release", "")

	f.fs.stats.Lock()
	f.fs.stats.OpenedFds--
	delete(f.fs.stats.FDIndex, f.fid)
	f.fs.stats.Unlock()

	f.r.Close()
}

func (f *File) Flush() fuse.Status {
	f.logOP("Flush", "")
	return fuse.OK
}

func (f *File) GetAttr(a *fuse.Attr) fuse.Status {
	f.logOP("FStat", "")
	a.Mode = fuse.S_IFREG | f.node.Mode()
	a.Size = uint64(f.node.Size)
	a.Ctime = f.node.Ctime()
	a.Mtime = f.node.Mtime()
	a.Owner = *fuse.CurrentOwner()
	return fuse.OK
}
