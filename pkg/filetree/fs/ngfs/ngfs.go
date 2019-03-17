package main

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
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

	"a4.io/blobstash/pkg/backend/s3/s3util"
	"a4.io/blobstash/pkg/client/blobstore"
	"a4.io/blobstash/pkg/client/clientutil"
	"a4.io/blobstash/pkg/client/kvstore"
	"a4.io/blobstash/pkg/config/pathutil"
	"a4.io/blobstash/pkg/ctxutil"
	rnode "a4.io/blobstash/pkg/filetree/filetreeutil/node"
	"a4.io/blobstash/pkg/filetree/reader/filereader"
	"a4.io/blobstash/pkg/filetree/writer"
	"a4.io/blobstash/pkg/iputil"
	"github.com/aws/aws-sdk-go/service/s3"
	lru "github.com/hashicorp/golang-lru"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/x/net/context"
)

var logger = log.New(os.Stderr, "BlobFS: ", log.LstdFlags)

const revisionHeader = "BlobStash-Filetree-FS-Revision"

var startedAt = time.Now()

func usage() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s MOUNTPOINT\n", os.Args[0])
	flag.PrintDefaults()
}

// Permissions bits for mode manipulation (borrowed from https://github.com/phayes/permbits/blob/master/permbits.go#L10)
const (
	setuid uint32 = 1 << (12 - 1 - iota)
	setgid
	sticky
	userRead
	userWrite
	userExecute
	groupRead
	groupWrite
	groupExecute
	otherRead
	otherWrite
	otherExecute
)

func main() {
	// Scans the arg list and sets up flags
	//debug := flag.Bool("debug", false, "print debugging messages.")
	resetCache := flag.Bool("reset-cache", false, "remove the local cache before starting.")
	syncDelay := flag.Duration("sync-delay", 5*time.Minute, "delay to wait after the last modification to initate a sync")
	forceRemote := flag.Bool("force-remote", false, "force fetching data blobs from object storage")
	disableRemote := flag.Bool("disable-remote", false, "disable fetching data blobs from object storage")
	configFile := flag.String("config-file", filepath.Join(pathutil.ConfigDir(), "fs_client.yaml"), "confg file path")
	configProfile := flag.String("config-profile", "default", "config profile name")

	flag.Usage = usage
	flag.Parse()

	if flag.NArg() != 2 {
		usage()
		os.Exit(2)
	}
	mountpoint := flag.Arg(0)
	ref := flag.Arg(1)

	profile, err := loadProfile(*configFile, *configProfile)
	if err != nil {
		fmt.Printf("failed to load config profile %s at %s: %v\n", *configProfile, *configFile, err)
		os.Exit(1)
	}

	if profile == nil {
		fmt.Printf("please setup a config file at %s\n", *configFile)
		os.Exit(1)
	}

	// Cache setup, follow XDG spec
	cacheDir := filepath.Join(pathutil.CacheDir(), "fs", fmt.Sprintf("%s_%s", mountpoint, ref))
	logger.Printf("cacheDir=%s\n", cacheDir)

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

	// Setup the clients for BlobStash
	hostname, err := os.Hostname()
	if err != nil {
		fmt.Printf("failed to get hostname: %v\n", err)
		os.Exit(1)
	}

	clientUtil := clientutil.NewClientUtil(profile.Endpoint,
		clientutil.WithAPIKey(profile.APIKey),
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

	// Discover server capabilities (for the remote/replication stuff)
	caps, err := clientUtil.Capabilities()
	if err != nil {
		log.Fatal(err)
	}

	isHostLocal, err := iputil.IsPrivate(profile.Endpoint)
	if err != nil {
		fmt.Printf("invalid BLOBS_API_HOST")
		os.Exit(1)
	}

	var useRemote bool
	switch {
	case *disableRemote, !caps.ReplicationEnabled:
		if *forceRemote {
			logger.Printf("WARNING: disabling remote as server does not support it\n")
		}
	case *forceRemote:
		useRemote = true
	case isHostLocal:
		useRemote = isHostLocal
	}

	c, err := fuse.Mount(
		mountpoint,
		fuse.VolumeName(filepath.Base(mountpoint)),
		fuse.NoAppleDouble(),
		fuse.NoAppleXattr(),
		fuse.MaxReadahead(32*1024*1024),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	// LRU cache for "data blobs" when reading a file
	freaderCache, err := lru.New(512)
	if err != nil {
		log.Fatal(err)
	}

	atCache, err := lru.NewARC(32)
	if err != nil {
		log.Fatal(err)
	}

	blobfs := &FS{
		profile:      profile,
		up:           writer.NewUploader(bs),
		clientUtil:   clientUtil,
		kvs:          kvs,
		ref:          ref,
		counters:     newCounters(),
		openedFds:    map[fuse.NodeID]*fdDebug{},
		openLogs:     []*fdDebug{},
		freaderCache: freaderCache,
		atCache:      atCache,
		caps:         caps,
		useRemote:    useRemote,
	}
	blobfs.bs, err = newCache(blobfs, bs, cacheDir)
	if err != nil {
		log.Fatal(err)
	}
	defer blobfs.bs.(*cache).Close()

	if profile.RemoteConfig != nil && profile.RemoteConfig.KeyFile != "" {
		var out [32]byte
		data, err := ioutil.ReadFile(profile.RemoteConfig.KeyFile)
		if err != nil {
			log.Fatal(err)
		}
		copy(out[:], data)
		blobfs.key = &out
		if profile.RemoteConfig.Endpoint != "" {
			blobfs.s3, err = s3util.NewWithCustomEndoint(profile.RemoteConfig.AccessKeyID, profile.RemoteConfig.SecretAccessKey, profile.RemoteConfig.Region, profile.RemoteConfig.Endpoint)
			if err != nil {
				log.Fatal(err)
			}
		} else {
			blobfs.s3, err = s3util.New(profile.RemoteConfig.Region)
			if err != nil {
				log.Fatal(err)
			}
		}
	}

	logger.Printf("caps=%+v use_remote=%v\n", caps, useRemote)

	go func() {
		ticker := time.NewTicker(45 * time.Second)
		for _ = range ticker.C {
			blobfs.muLastRev.Lock()
			currentRev := blobfs.lastRevision
			blobfs.muLastRev.Unlock()

			if currentRev > 0 && currentRev > blobfs.lastSyncRev {
				logger.Printf("sync ticker, current_rev=%d last_sync_rev=%d\n", currentRev, blobfs.lastSyncRev)
				if time.Now().UTC().Sub(time.Unix(0, currentRev)) > *syncDelay {
					if err := blobfs.GC(); err != nil {
						panic(err)
					}
				}
			}
		}
	}()

	go func() {
		err = fs.Serve(c, blobfs)
		if err != nil {
			log.Fatal(err)
		}

		// check if the mount process has an error to report
		<-c.Ready
		if err := c.MountError; err != nil {
			log.Fatal(err)
		}
	}()

	cs := make(chan os.Signal, 1)
	signal.Notify(cs, os.Interrupt,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	<-cs
	logger.Printf("GC...")
	if err := blobfs.GC(); err != nil {
		fmt.Printf("\nGC failed err=%v\n", err)
	} else {
		fmt.Printf("done\n")
	}
	logger.Printf("Unmounting...\n")
	fuse.Unmount(mountpoint)
}

// blobStore is the blobstore client interface
type blobStore interface {
	Stat(ctx context.Context, hash string) (bool, error)
	Get(ctx context.Context, hash string) ([]byte, error)
	Put(ctx context.Context, hash string, data []byte) error
}

// FS implements the BlobStash FileTree filesystem
type FS struct {
	// BlobStash clients (and cache)
	up         *writer.Uploader
	kvs        *kvstore.KvStore
	bs         blobStore
	clientUtil *clientutil.ClientUtil
	caps       *clientutil.Caps

	// config profile
	profile   *profile
	useRemote bool

	// S3 client and key
	s3  *s3.S3
	key *[32]byte

	// cached root dir
	ref    string
	ftRoot *dir

	// "magic" root
	root *fs.Tree

	// in-mem blobs cache
	freaderCache *lru.Cache

	// in-mem cache for RO snapshots of older versions
	atCache *lru.ARCCache

	// debug info
	counters  *counters
	openedFds map[fuse.NodeID]*fdDebug
	openLogs  []*fdDebug
	mu        sync.Mutex

	// current revision
	lastRevision int64
	lastSyncRev  int64
	muLastRev    sync.Mutex
}

// FIXME(tsileo): use it, and a ticker with a debounce
func (fs *FS) updateLastRevision(resp *http.Response) {
	fs.muLastRev.Lock()
	defer fs.muLastRev.Unlock()
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

// Save the current tree and reset the stash
func (fs *FS) GC() error {
	fs.muLastRev.Lock()
	defer fs.muLastRev.Unlock()

	if fs.lastRevision == 0 {
		return nil
	}

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

	// FIXME(tsileo): make the stash name configurable
	resp, err := fs.clientUtil.PostMsgpack(
		fmt.Sprintf("/api/stash/rwfs-%s/_gc", fs.ref),
		map[string]interface{}{
			"script": gcScript,
		},
	)
	if err != nil {
		// FIXME(tsileo): find a better way to handle this?
		return err
	}
	defer resp.Body.Close()

	if err := clientutil.ExpectStatusCode(resp, http.StatusNoContent); err != nil {
		if err.IsNotFound() {
			// The stash does not exists, nothing to GC
			return nil
		}
		// FIXME(tsileo): find a better way to handle this?
		return err
	}

	fs.lastSyncRev = fs.lastRevision

	return nil
}

// remotePath the API path for the FileTree API
func (fs *FS) remotePath(path string) string {
	return fmt.Sprintf("/api/filetree/fs/fs/%s/%s", fs.ref, path[1:])
}

// getNode fetches the node at path from BlobStash, like a "remote stat".
func (fs *FS) getNode(path string, asOf int64) (*node, error) {
	return fs.getNodeAsOf(path, 1, asOf)
}

// getNode fetches the node at path from BlobStash, like a "remote stat".
func (fs *FS) getNodeAsOf(path string, depth int, asOf int64) (*node, error) {
	// Fetch the node via the FileTree FS API
	resp, err := fs.clientUtil.Get(
		fs.remotePath(path)+fmt.Sprintf("?depth=%d", depth),
		clientutil.WithQueryArg("as_of", strconv.FormatInt(asOf, 10)),
	)
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

	node := &node{}
	if err := clientutil.Unmarshal(resp, node); err != nil {
		return nil, err
	}

	node.AsOf = asOf
	// fmt.Printf("getNode(%s) = %v\n", fs.remotePath(path), node)

	return node, nil
}

// Root returns the root node of the FS
func (cfs *FS) Root() (fs.Node, error) {
	// Check if there's a cached root
	if cfs.root != nil {
		return cfs.root, nil
	}

	// Create a dummy dir that will be our root ref
	cfs.root = &fs.Tree{}

	// the read-write root will be mounted a /current
	ftRoot, err := cfs.FTRoot()
	if err != nil {
		return nil, err
	}
	cfs.root.Add("current", ftRoot)

	// magic dir that list all versions, YYYY-MM-DDTHH:MM:SS
	cfs.root.Add("versions", &versionsDir{cfs})

	// Time travel magic dir: "at/2018/myfile", "at/-40h/myfile"
	cfs.root.Add("at", &atDir{cfs})

	// Last 100 opened files (locally tracked only)
	cfs.root.Add("recent", &recentDir{cfs, &cfs.openLogs})

	// Debug VFS mounted a /.stats
	cfs.root.Add(".stats", statsTree(cfs))

	return cfs.root, nil
}

// FTRoot returns the FileTree node root
func (fs *FS) FTRoot() (fs.Node, error) {
	// Check if there's a cached root
	if fs.ftRoot != nil {
		return fs.ftRoot, nil
	}

	// Create a dummy dir that will be our root ref
	fs.ftRoot = &dir{
		path: "/",
		fs:   fs,
		node: nil,
	}

	// Actually loads it
	if err := fs.ftRoot.preloadFTRoot(); err != nil {
		return nil, err
	}
	return fs.ftRoot, nil
}

// dir implements fs.Node and represents a FileTree directory
type dir struct {
	path string
	fs   *FS
	node *node

	ro   bool
	asOf int64

	mu       sync.Mutex
	children map[string]fs.Node
	parent   *dir
}

var _ fs.Node = (*dir)(nil)
var _ fs.NodeMkdirer = (*dir)(nil)
var _ fs.NodeCreater = (*dir)(nil)
var _ fs.NodeRemover = (*dir)(nil)
var _ fs.HandleReadDirAller = (*dir)(nil)
var _ fs.NodeStringLookuper = (*dir)(nil)
var _ fs.NodeListxattrer = (*dir)(nil)
var _ fs.NodeGetxattrer = (*dir)(nil)

// FTNode lazy-loads the node from BlobStash FileTree API
func (d *dir) FTNode() (*node, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	//if d.node != nil {
	//	return d.node, nil
	//}
	n, err := d.fs.getNode(d.path, d.asOf)
	if err != nil {
		return nil, err
	}
	d.node = n
	return n, nil
}

// Listxattr implements the fs.NodeListxattrer interface
func (d *dir) Listxattr(ctx context.Context, req *fuse.ListxattrRequest, resp *fuse.ListxattrResponse) error {
	logger.Printf("Listxattr %s", d.path)
	// TODO(tsileo): node info + metadata support
	resp.Append([]string{"debug.ref"}...)
	return nil
}

// Getxattr implements the fs.NodeGetxattrer interface
func (d *dir) Getxattr(ctx context.Context, req *fuse.GetxattrRequest, resp *fuse.GetxattrResponse) error {
	logger.Printf("Getxattr %s", d.path)
	switch req.Name {
	case "debug.ref":
		resp.Xattr = []byte(d.node.Ref)
	default:
		return fuse.ErrNoXattr
	}
	return nil
}

// Attr implements the fs.Node interface
func (d *dir) Attr(ctx context.Context, a *fuse.Attr) error {
	logger.Printf("Attr %s", d.path)
	n, err := d.FTNode()
	if err != nil {
		return err
	}
	a.Valid = 0 * time.Second
	a.Uid = uint32(os.Getuid())
	a.Gid = uint32(os.Getgid())

	if d.path == "/" {
		a.Inode = 1
	}
	if n != nil {
		a.Mode = os.ModeDir | os.FileMode(n.mode())
	} else {
		a.Mode = os.ModeDir | 0755
	}

	if d.ro || d.asOf > 0 {
		a.Mode &^= os.FileMode(userWrite | groupWrite | otherWrite)
	}

	return nil
}

// Special preloading for the root that fetch the root tree with a depth of 2
// (meaning we fetch the directories of the directories inside the root).
// The root will be cached, and the same struct will always be returned.
func (d *dir) preloadFTRoot() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Fetch the root node with a depth=2
	n, err := d.fs.getNodeAsOf(d.path, 2, d.asOf)
	if err != nil {
		return err
	}
	// Cache the node
	d.node = n
	if d.node == nil {
		return nil
	}

	d.children = map[string]fs.Node{}
	for _, child := range d.node.Children {
		child.AsOf = d.asOf
		// We can set the node directly, and directories will contains children because we asked
		// for a depth=2 when requesting the root dir
		if child.isFile() {
			d.children[child.Name] = &file{
				path:   filepath.Join(d.path, child.Name),
				fs:     d.fs,
				node:   child,
				parent: d,
				ro:     d.ro,
			}
		} else {
			d.children[child.Name] = &dir{
				path:   filepath.Join(d.path, child.Name),
				fs:     d.fs,
				node:   child,
				parent: d,
				ro:     d.ro,
			}
			// "load"/setup the children index, as we already have the children within the node
			d.children[child.Name].(*dir).loadChildren()
		}
	}

	return nil
}

// Load the children from the FileTree node to the fs.Node children index used for lookups and readdiralls
func (d *dir) loadChildren() {
	d.children = map[string]fs.Node{}
	for _, child := range d.node.Children {
		child.AsOf = d.asOf
		if child.isFile() {
			d.children[child.Name] = &file{
				path:   filepath.Join(d.path, child.Name),
				fs:     d.fs,
				node:   child,
				parent: d,
				ro:     d.ro,
			}
		} else {
			// The node is set to nil for directories because we haven't fetched to children
			d.children[child.Name] = &dir{
				path:   filepath.Join(d.path, child.Name),
				fs:     d.fs,
				node:   nil,
				parent: d,
				ro:     d.ro,
			}
		}
	}

}

// Lookup implements the fs.NodeRequestLookuper interface
func (d *dir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	// lazy load the remote node
	n, err := d.FTNode()
	if err != nil {
		return nil, err
	}
	if n == nil {
		return nil, fuse.ENOENT
	}

	// fetch the children (local index)
	if d.children == nil {
		d.loadChildren()
	}

	// update the index
	d.mu.Lock()
	defer d.mu.Unlock()
	if node, ok := d.children[name]; ok {
		return node, nil
	}

	return nil, fuse.ENOENT
}

// ReadDirAll implements the fs.HandleReadDirAller interface
func (d *dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	// lazy loads the remote node
	n, err := d.FTNode()
	if err != nil {
		return nil, err
	}
	if n == nil {
		return nil, fuse.ENOENT
	}
	// load the children (local index)
	if d.children == nil {
		d.loadChildren()
	}

	// Build the response
	d.mu.Lock()
	defer d.mu.Unlock()
	out := []fuse.Dirent{}
	for _, child := range d.children {
		if f, ok := child.(*file); ok {
			out = append(out, fuse.Dirent{Name: filepath.Base(f.path), Type: fuse.DT_File})
		} else {
			d := child.(*dir)
			out = append(out, fuse.Dirent{Name: filepath.Base(d.path), Type: fuse.DT_Dir})

		}
	}

	return out, nil
}

// Mkdir implements the fs.NodeMkdirer interface
func (d *dir) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	if d.ro || d.asOf > 0 {
		return nil, fuse.EPERM
	}

	// new mtime for the parent dir
	mtime := time.Now().Unix()

	// initialize an empty dir node
	node := &rnode.RawNode{
		Version: rnode.V1,
		Type:    rnode.Dir,
		Name:    req.Name,
		ModTime: mtime,
	}

	// patch dir to insert the new empty dir
	resp, err := d.fs.clientUtil.PatchMsgpack(
		d.fs.remotePath(d.path),
		node,
		clientutil.WithQueryArg("mtime", strconv.FormatInt(mtime, 10)),
	)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if err := clientutil.ExpectStatusCode(resp, http.StatusOK); err != nil {
		return nil, err
	}

	// Update the FS "revision" (the kv entry version) for later GC
	d.fs.updateLastRevision(resp)

	// initialize the FS node and update the local dir
	newDir := &dir{path: filepath.Join(d.path, req.Name), fs: d.fs, node: nil, parent: d}
	d.mu.Lock()
	if d.children == nil {
		d.children = map[string]fs.Node{}
	}
	d.children[req.Name] = newDir
	d.mu.Unlock()

	return newDir, nil
}

// Rename implements the fs.NodeRenamer interface
func (d *dir) Rename(ctx context.Context, req *fuse.RenameRequest, newDir fs.Node) error {
	if d.ro || d.asOf > 0 {
		return fuse.EPERM
	}

	// mtime for the modifications
	mtime := time.Now().Unix()

	d.mu.Lock()
	n := d.children[req.OldName]
	d.mu.Unlock()

	// First, we remove the old path
	resp, err := d.fs.clientUtil.Delete(
		d.fs.remotePath(filepath.Join(d.path, req.OldName)),
		clientutil.WithQueryArg("mtime", strconv.FormatInt(mtime, 10)),
	)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if err := clientutil.ExpectStatusCode(resp, http.StatusNoContent); err != nil {
		return err
	}

	newPath := filepath.Join(newDir.(*dir).path, req.NewName)
	fmt.Printf("NewName=%s\n", newPath)

	var ref string
	if d, ok := n.(*dir); ok {
		ref = d.node.Ref

	} else {
		f := n.(*file)
		ref = f.node.Ref
	}

	// Next, we re-add it to its dest
	resp, err = d.fs.clientUtil.PatchMsgpack(
		d.fs.remotePath(newDir.(*dir).path),
		nil,
		clientutil.WithHeaders(map[string]string{
			"BlobStash-Filetree-Patch-Ref":  ref,
			"BlobStash-Filetree-Patch-Name": filepath.Base(newPath),
		}),
		clientutil.WithQueryArgs(map[string]string{
			// FIXME(tsileo): s/rename/change/ ?
			"rename": strconv.FormatBool(true),
			"mtime":  strconv.Itoa(int(mtime)),
		}),
	)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if err := clientutil.ExpectStatusCode(resp, http.StatusOK); err != nil {
		return err
	}

	// Update the FS "revision" (the kv entry version) for later GC
	d.fs.updateLastRevision(resp)

	d.mu.Lock()
	delete(d.children, req.OldName)
	d.mu.Unlock()
	if d, ok := n.(*dir); ok {
		d.path = newPath
		d.node = nil
		if _, err := d.FTNode(); err != nil {
			return err
		}
	} else {
		f := n.(*file)
		f.path = newPath
		f.node = nil
		if _, err := f.FTNode(); err != nil {
			return err
		}
	}
	d2 := newDir.(*dir)
	d2.mu.Lock()
	d2.children[req.NewName] = n
	d2.mu.Unlock()

	fmt.Printf("Rename done, new node=%+v\n", n)
	return nil
}

// Create implements the fs.NodeCreater interface
func (d *dir) Create(ctx context.Context, req *fuse.CreateRequest, res *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	if d.ro || d.asOf > 0 {
		return nil, nil, fuse.EPERM
	}

	d.fs.counters.incr("open")
	d.fs.counters.incr("open-rw")

	// mtime for the parent dir
	mtime := time.Now().Unix()

	// Initialize an empty file node
	node := &rnode.RawNode{
		Type:    rnode.File,
		Name:    req.Name,
		Version: rnode.V1,
		ModTime: mtime,
		Mode:    uint32(req.Mode),
	}

	// Patch the parent dir
	resp, err := d.fs.clientUtil.PatchMsgpack(
		d.fs.remotePath(d.path),
		node,
		clientutil.WithQueryArg("mtime", strconv.FormatInt(mtime, 10)),
	)
	if err != nil {
		d.fs.counters.incr("open-rw-error")
		return nil, nil, err
	}
	defer resp.Body.Close()

	if err := clientutil.ExpectStatusCode(resp, http.StatusOK); err != nil {
		d.fs.counters.incr("open-rw-error")
		return nil, nil, err
	}

	// Update the FS "revision" (the kv entry version) for later GC
	d.fs.updateLastRevision(resp)

	// Initialize the file node
	f := &file{
		path:   filepath.Join(d.path, req.Name),
		fs:     d.fs,
		node:   nil,
		parent: d,
	}

	// Update the local dir
	d.mu.Lock()
	if d.children == nil {
		d.children = map[string]fs.Node{}
	}
	d.children[req.Name] = f
	f.fds++

	d.fs.openedFds[req.Node] = &fdDebug{
		Path:     d.path,
		PID:      req.Pid,
		PName:    getProcName(req.Pid),
		RW:       true,
		openedAt: time.Now(),
	}
	d.fs.openLogs = append(d.fs.openLogs, d.fs.openedFds[req.Node])
	if len(d.fs.openLogs) > 100 {
		d.fs.openLogs = d.fs.openLogs[:100]
	}
	d.mu.Unlock()

	// Initialize a temporary file for the RW handle
	tmp, err := ioutil.TempFile("", fmt.Sprintf("blobfs-%s-", req.Name))
	if err != nil {
		d.fs.counters.incr("open-rw-error")
		return nil, nil, err
	}

	// Initialize the RW handle
	fh := &rwFileHandle{
		f:   f,
		tmp: tmp,
	}
	f.h = fh

	return f, fh, nil
}

// Remove implements the fs.NodeRemover interface
func (d *dir) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	if d.ro || d.asOf > 0 {
		return fuse.EPERM
	}

	// mtime for the parent dir
	mtime := time.Now().Unix()

	// Remove the node from the dir in the index server/BlobStash
	resp, err := d.fs.clientUtil.Delete(
		d.fs.remotePath(filepath.Join(d.path, req.Name)),
		clientutil.WithQueryArg("mtime", strconv.FormatInt(mtime, 10)),
	)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if err := clientutil.ExpectStatusCode(resp, http.StatusNoContent); err != nil {
		fmt.Printf("err=%+v\n", err)
		return err
	}

	// Update the FS "revision" (the kv entry version) for later GC
	d.fs.updateLastRevision(resp)

	// Update the local node
	d.mu.Lock()
	delete(d.children, req.Name)
	d.mu.Unlock()

	return nil
}

// file implements both Node and Handle for the hello file.
type file struct {
	// absolute path
	path string

	// read-only mode
	ro   bool
	asOf int64

	// FS ref
	fs *FS

	// FileTree node
	node *node

	// Node parent
	parent *dir

	// Guard the rw handle and the file descriptor count
	mu sync.Mutex

	// Keep track of the opened file descriptors
	fds int
	h   *rwFileHandle
}

var _ fs.Node = (*file)(nil)
var _ fs.NodeAccesser = (*file)(nil)
var _ fs.NodeSetattrer = (*file)(nil)
var _ fs.NodeOpener = (*file)(nil)
var _ fs.NodeFsyncer = (*file)(nil)
var _ fs.NodeListxattrer = (*file)(nil)
var _ fs.NodeGetxattrer = (*file)(nil)

// Fsync implements the fs.NodeFsyncer interface
func (f *file) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	return nil
}

// FTNode lazy-loads the node from BlobStash FileTree API
func (f *file) FTNode() (*node, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Returns the cached node if it's already there
	if f.node != nil {
		return f.node, nil
	}

	// Loads it from BlobStash
	n, err := f.fs.getNode(f.path, f.asOf)
	if err != nil {
		return nil, err
	}

	// Cache it
	f.node = n
	return n, nil
}

// Attr implements the fs.Node interface
func (f *file) Attr(ctx context.Context, a *fuse.Attr) error {
	n, err := f.FTNode()
	if err != nil {
		return err
	}
	a.Valid = 0 * time.Second
	a.Uid = uint32(os.Getuid())
	a.Gid = uint32(os.Getgid())
	a.BlockSize = 4096

	if f.h != nil {
		fi, err := f.h.tmp.Stat()
		if err != nil {
			return err
		}
		a.Mode = fi.Mode()
		a.Size = uint64(fi.Size())
		a.Mtime = fi.ModTime()
		a.Ctime = fi.ModTime()
	} else {

		// a.Inode = 2
		if n != nil {
			a.Mode = os.FileMode(n.mode()) | 0644
			a.Size = uint64(n.Size)
			a.Mtime = time.Unix(int64(n.mtime()), 0)
			a.Ctime = time.Unix(int64(n.ctime()), 0)
		} else {
			a.Mode = 0644
			a.Size = 0
		}
	}
	if a.Size > 0 {
		a.Blocks = a.Size/512 + 1
	}

	// the file is read-only, remove the write bits
	if f.ro || f.asOf > 0 {
		a.Mode &^= os.FileMode(userWrite | groupWrite | otherWrite)
	}

	return nil
}

// Listxattr implements the fs.NodeListxattrer interface
func (f *file) Listxattr(ctx context.Context, req *fuse.ListxattrRequest, resp *fuse.ListxattrResponse) error {
	// Node metadata (stored in the node/meta itself)
	for k, _ := range f.node.Metadata {
		resp.Append(fmt.Sprintf("metadata.%s", k))
	}
	// Node info (video)
	if v, vok := f.node.Info["video"]; vok {
		for k, _ := range v.(map[string]interface{}) {
			resp.Append(fmt.Sprintf("info.video.%s", k))
		}
	}
	// Node info (image)
	if v, vok := f.node.Info["image"]; vok {
		for k, _ := range v.(map[string]interface{}) {
			resp.Append(fmt.Sprintf("info.image.%s", k))
		}
	}
	// Node debug
	resp.Append([]string{"debug.ref"}...)
	return nil
}

// Getxattr implements the fs.NodeGetxattrer interface
func (f *file) Getxattr(ctx context.Context, req *fuse.GetxattrRequest, resp *fuse.GetxattrResponse) error {
	switch {
	case req.Name == "debug.ref":
		resp.Xattr = []byte(f.node.Ref)
	case strings.HasPrefix(req.Name, "metadata."):
		resp.Xattr = []byte(fmt.Sprintf("%v", f.node.Metadata[req.Name[9:]]))
	case strings.HasPrefix(req.Name, "info.video."):
		resp.Xattr = []byte(fmt.Sprintf("%v", f.node.Info["video"].(map[string]interface{})[req.Name[11:]]))
	case strings.HasPrefix(req.Name, "info.image."):
		resp.Xattr = []byte(fmt.Sprintf("%v", f.node.Info["image"].(map[string]interface{})[req.Name[11:]]))
	default:
		return fuse.ErrNoXattr
	}
	return nil
}

// Access implements the fs.NodeAccesser interface
func (f *file) Access(ctx context.Context, req *fuse.AccessRequest) error {
	return nil
}

// Setattr implements the fs.NodeSetattrer
func (f *file) Setattr(ctx context.Context, req *fuse.SetattrRequest, res *fuse.SetattrResponse) error {
	if f.ro || f.asOf > 0 {
		return fuse.EPERM
	}

	n, err := f.FTNode()
	if err != nil {
		return err
	}
	if n == nil {

	} else {

		mtime := time.Now().Unix()
		headers := map[string]string{
			"BlobStash-Filetree-Patch-Ref": n.Ref,
		}
		if req.Valid&fuse.SetattrMtime != 0 {
			mtime = req.Mtime.Unix()
		}
		//if req.Valid&fuse.SetattrAtime != 0 {
		//	n.atime = req.Atime
		//}
		if req.Valid&fuse.SetattrMode != 0 {
			headers["BlobStash-Filetree-Patch-Mode"] = strconv.Itoa(int(req.Mode))
		}

		resp, err := f.fs.clientUtil.PatchMsgpack(
			f.fs.remotePath(filepath.Dir(f.path)),
			nil,
			clientutil.WithQueryArgs(map[string]string{
				"mtime": strconv.Itoa(int(mtime)),
			}),
			clientutil.WithHeaders(headers),
		)
		if err != nil {
			return err
		}

		defer resp.Body.Close()

		if err := clientutil.ExpectStatusCode(resp, http.StatusOK); err != nil {
			return err
		}

		// Update the FS "revision" (the kv entry version) for later GC
		f.fs.updateLastRevision(resp)

	}
	// TODO(tsileo): apply the attrs to the temp file
	f.Attr(ctx, &res.Attr)
	return nil
}

// Open implements the fs.HandleOpener interface
func (f *file) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	fmt.Printf("Open %v %+v %s write=%v\n", f, f.node, f.path, req.Flags&fuse.OpenFlags(os.O_WRONLY|os.O_RDWR|os.O_APPEND|os.O_CREATE) != 0)
	fmt.Printf("current handler=%+v\n", f.h)

	isRW := req.Flags&fuse.OpenFlags(os.O_WRONLY|os.O_RDWR|os.O_APPEND|os.O_CREATE) != 0
	if (f.ro || f.asOf > 0) && isRW {
		return nil, fuse.EPERM
	}

	f.fs.counters.incr("open")
	if isRW {
		f.fs.counters.incr("open-rw")
	} else {
		f.fs.counters.incr("open-ro")
	}

	// Update the opened file descriptor counter
	f.fds++
	f.fs.mu.Lock()
	f.fs.openedFds[req.Node] = &fdDebug{
		Path:     f.path,
		PID:      req.Pid,
		PName:    getProcName(req.Pid),
		RW:       isRW,
		openedAt: time.Now(),
	}
	f.fs.openLogs = append([]*fdDebug{f.fs.openedFds[req.Node]}, f.fs.openLogs...)
	if len(f.fs.openLogs) > 100 {
		f.fs.openLogs = f.fs.openLogs[:100]
	}
	f.fs.mu.Unlock()

	// Short circuit the open if this file is already open for write
	if f.h != nil {
		fmt.Printf("Returning already openfile\n")
		return f.h, nil
	}

	// Lazy loads the remote node if needed
	if _, err := f.FTNode(); err != nil {
		if isRW {
			f.fs.counters.incr("open-rw-error")
		} else {
			f.fs.counters.incr("open-ro-error")
		}

		return nil, err
	}

	// Open RW
	if req.Flags&fuse.OpenFlags(os.O_WRONLY|os.O_RDWR|os.O_APPEND|os.O_CREATE) != 0 {
		// Create a temporary file
		tmp, err := ioutil.TempFile("", fmt.Sprintf("blobfs-%s-", filepath.Base(f.path)))
		if err != nil {
			f.fs.counters.incr("open-rw-error")
			return nil, err
		}

		// Initialize a reader for initializing/loading the node content into the temp file
		r, err := f.Reader()
		if err != nil {
			f.fs.counters.incr("open-rw-error")
			return nil, err
		}

		// Copy the reader into the temp file if needed
		f.mu.Lock()
		defer f.mu.Unlock()

		if r != nil {
			defer r.Close()

			if _, err := io.Copy(tmp, r); err != nil {
				f.fs.counters.incr("open-rw-error")
				return nil, err
			}
		}

		// Initialize the RW handler
		rwHandle := &rwFileHandle{
			f:   f,
			tmp: tmp,
		}
		f.h = rwHandle
		return rwHandle, nil
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.node == nil {
		return nil, fuse.ENOENT
	}

	var r fileReader
	if f.h != nil {
		r = f.h.tmp
	}

	// Initialize a RO handle
	fh := &fileHandle{
		f: f,
		r: r,
	}
	resp.Flags |= fuse.OpenKeepCache
	return fh, nil
}

// fileReader is the minimal interface for the file hander
type fileReader interface {
	io.Reader
	io.ReaderAt
	io.Closer
}

type preloadableFileReader interface {
	fileReader
	PreloadChunks()
}

// fileHandle implements a RO file handler
type fileHandle struct {
	f *file
	r fileReader
}

var _ fs.HandleReader = (*fileHandle)(nil)
var _ fs.HandleReleaser = (*fileHandle)(nil)

// Reader returns a fileReader for the remote node
func (f *file) Reader() (fileReader, error) {
	// Fetch the remote node
	n, err := f.FTNode()
	if err != nil {
		return nil, err
	}
	if n == nil {
		return nil, nil
	}

	// Fetch the reference blob to decode the "raw meta"
	blob, err := f.fs.bs.Get(context.Background(), n.Ref)
	if err != nil {
		return nil, err
	}
	meta, err := rnode.NewNodeFromBlob(n.Ref, blob)
	if err != nil {
		return nil, fmt.Errorf("failed to build node from blob \"%s\": %v", blob, err)
	}

	// Instanciate the filereader
	var fr preloadableFileReader
	logger.Printf("use_remote=%v remote_refs=%+v\n", f.fs.useRemote, n.RemoteRefs)
	if f.fs.useRemote && n.RemoteRefs != nil {
		logger.Println("opening file with remote")
		fr = filereader.NewFileRemote(context.Background(), f.fs.bs, meta, n.RemoteRefs, f.fs.freaderCache)
	} else {
		fr = filereader.NewFile(context.Background(), f.fs.bs, meta, f.fs.freaderCache)
	}

	// FIXME(tsileo): test if preloading is worth it
	// fr.PreloadChunks()

	return fr, nil
}

// Release implements the fs.HandleReleaser interface
func (fh *fileHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	fh.f.mu.Lock()
	defer fh.f.mu.Unlock()

	// Close the reader if it was opened
	if fh.r != nil {
		fh.r.Close()
		fh.r = nil
	}

	// Update the opened file descriptor counter
	// TODO(tsileo): release the rwFileHandler here too if it was used?
	fh.f.fds--
	fh.f.fs.mu.Lock()
	if _, ok := fh.f.fs.openedFds[req.Node]; ok {
		delete(fh.f.fs.openedFds, req.Node)
	}
	fh.f.fs.mu.Unlock()

	return nil
}

// Read implements the fs.HandleReader interface
func (fh *fileHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	logger.Printf("Read [ro] %s: %#v", fh.f.path, req)
	var err error
	var r fileReader
	if fh.f.h != nil {
		// Short circuit the read operation to the RW handle
		r = fh.f.h.tmp
	} else {
		// Shortcut for empty file
		if fh.f.node.Size == 0 {
			return nil
		}

		// Lazy-loads the reader
		if fh.r != nil {
			r = fh.r
		} else {
			r, err = fh.f.Reader()
			if err != nil {
				return err
			}
			fh.r = r
		}
	}

	// No reader, the file was just created
	if r == nil {
		return nil
	}

	// Perform the read operation on the fileReader
	buf := make([]byte, req.Size)
	n, err := r.ReadAt(buf, req.Offset)
	if err != nil {
		return err
	}
	resp.Data = buf[:n]
	return nil
}

// rwFileHandle implements a RW file handler
type rwFileHandle struct {
	f *file

	tmp *os.File
}

var _ fs.HandleFlusher = (*rwFileHandle)(nil)
var _ fs.HandleReader = (*rwFileHandle)(nil)
var _ fs.HandleWriter = (*rwFileHandle)(nil)
var _ fs.HandleReleaser = (*rwFileHandle)(nil)

// Read implements the fs.HandleReader interface
func (f *rwFileHandle) Read(ctx context.Context, req *fuse.ReadRequest, res *fuse.ReadResponse) error {
	buf := make([]byte, req.Size)
	n, err := f.tmp.ReadAt(buf, req.Offset)

	switch err {
	case nil:
	case io.EOF:
		err = nil
	default:
		return err
	}

	res.Data = buf[:n]
	return nil
}

// Write implements the fs.HandleWriter interface
func (f *rwFileHandle) Write(ctx context.Context, req *fuse.WriteRequest, res *fuse.WriteResponse) error {
	n, err := f.tmp.WriteAt(req.Data, req.Offset)
	if err != nil {
		return err
	}
	res.Size = n
	return nil
}

// Flush implements the fs.HandleFlusher interface
func (f *rwFileHandle) Flush(ctx context.Context, req *fuse.FlushRequest) error {
	// Upload the file
	f.f.mu.Lock()
	rawNode, err := f.f.fs.up.PutFileRename(f.tmp.Name(), filepath.Base(f.f.path), true)
	if err != nil {
		return nil
	}
	f.f.mu.Unlock()

	// Patch the parent dir
	resp, err := f.f.fs.clientUtil.PatchMsgpack(
		f.f.fs.remotePath(filepath.Dir(f.f.path)),
		rawNode,
		clientutil.WithQueryArgs(map[string]string{
			"mtime": strconv.Itoa(int(rawNode.ModTime)),
		}))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if err := clientutil.ExpectStatusCode(resp, http.StatusOK); err != nil {
		return err
	}

	// Update the FS "revision" (the kv entry version) for later GC
	f.f.fs.updateLastRevision(resp)

	// Reset the cached FileTree node
	f.f.node = nil
	if _, err := f.f.FTNode(); err != nil {
		return err
	}

	return nil
}

// Release implements the fuse.HandleReleaser interface
func (f *rwFileHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	f.f.mu.Lock()
	defer f.f.mu.Unlock()

	f.f.fds--
	f.f.fs.mu.Lock()
	if _, ok := f.f.fs.openedFds[req.Node]; ok {
		delete(f.f.fs.openedFds, req.Node)
	}
	f.f.fs.mu.Unlock()

	if f.f.fds == 0 {
		f.tmp.Close()
		if err := os.Remove(f.tmp.Name()); err != nil {
			return err
		}
	}
	f.f.h = nil
	return nil
}
