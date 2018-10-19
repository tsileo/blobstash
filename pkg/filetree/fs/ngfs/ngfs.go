package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"text/tabwriter"
	"time"

	"a4.io/blobstash/pkg/client/blobstore"
	"a4.io/blobstash/pkg/client/clientutil"
	"a4.io/blobstash/pkg/client/kvstore"
	"a4.io/blobstash/pkg/config/pathutil"
	"a4.io/blobstash/pkg/ctxutil"
	rnode "a4.io/blobstash/pkg/filetree/filetreeutil/node"
	"a4.io/blobstash/pkg/filetree/reader/filereader"
	"a4.io/blobstash/pkg/filetree/writer"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/x/net/context"
	yaml "gopkg.in/yaml.v2"
)

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

// RemoteConfig holds the "remote endpoint" configuration
type RemoteConfig struct {
	Endpoint        string `yaml:"endpoint"`
	Region          string `yaml:"region"`
	Bucket          string `yaml:"bucket"`
	AccessKeyID     string `yaml:"access_key_id"`
	SecretAccessKey string `yaml:"secret_access_key"`
	KeyFile         string `yaml:"key_file"`
}

// Profile holds a profile configuration
type Profile struct {
	RemoteConfig *RemoteConfig `yaml:"remote_config"`
	Endpoint     string        `yaml:"endpoint"`
	APIKey       string        `yaml:"api_key"`
}

// Config holds config profiles
type Config map[string]*Profile

// loadProfile loads the config file and the given profile within it
func loadProfile(configFile, name string) (*Profile, error) {
	dat, err := ioutil.ReadFile(configFile)
	switch {
	case err == nil:
	case os.IsNotExist(err):
		return nil, nil
	default:
		return nil, err
	}
	out := Config{}
	if err := yaml.Unmarshal(dat, out); err != nil {
		return nil, err
	}

	prof, ok := out[name]
	if !ok {
		return nil, fmt.Errorf("profile %s not found", name)
	}

	return prof, nil
}

const revisionHeader = "BlobStash-Filetree-FS-Revision"

func main() {
	// Scans the arg list and sets up flags
	//debug := flag.Bool("debug", false, "print debugging messages.")
	resetCache := flag.Bool("reset-cache", false, "remove the local cache before starting.")
	//roMode := flag.Bool("ro", false, "read-only mode")
	//syncDelay := flag.Duration("sync-delay", 5*time.Minute, "delay to wait after the last modification to initate a sync")
	//forceRemote := flag.Bool("force-remote", false, "force fetching data blobs from object storage")
	//disableRemote := flag.Bool("disable-remote", false, "disable fetching data blobs from object storage")
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
	fmt.Printf("cacheDir=%s\n", cacheDir)

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

	c, err := fuse.Mount(
		mountpoint,
		fuse.VolumeName(filepath.Base(mountpoint)),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	blobfs := &FS{
		up:         writer.NewUploader(bs),
		clientUtil: clientUtil,
		kvs:        kvs,
		ref:        ref,
		counters:   newCounters(),
		openedFds:  map[fuse.NodeID]*fdDebug{},
		openLogs:   []*fdDebug{},
	}
	blobfs.bs, err = newCache(bs, cacheDir)
	if err != nil {
		log.Fatal(err)
	}
	err = fs.Serve(c, blobfs)
	if err != nil {
		log.Fatal(err)
	}

	// check if the mount process has an error to report
	<-c.Ready
	if err := c.MountError; err != nil {
		log.Fatal(err)
	}
}

// blobStore is the blobstore client interface
type blobStore interface {
	Stat(ctx context.Context, hash string) (bool, error)
	Get(ctx context.Context, hash string) ([]byte, error)
	Put(ctx context.Context, hash string, data []byte) error
}

// FS implements the BlobStash FileTree filesystem
type FS struct {
	up         *writer.Uploader
	kvs        *kvstore.KvStore
	bs         blobStore
	clientUtil *clientutil.ClientUtil
	ref        string
	root       *fs.Tree
	ftRoot     *dir

	openedFds map[fuse.NodeID]*fdDebug
	openLogs  []*fdDebug
	mu        sync.Mutex

	counters *counters
}

// remotePath the API path for the FileTree API
func (fs *FS) remotePath(path string) string {
	return fmt.Sprintf("/api/filetree/fs/fs/%s/%s", fs.ref, path[1:])
}

// getNode fetches the node at path from BlobStash, like a "remote stat".
func (fs *FS) getNode(path string) (*node, error) {
	return fs.getNodeAsOf(path, 1, 0)
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
	fmt.Printf("getNode(%s) = %v\n", fs.remotePath(path), node)

	return node, nil
}

// Root returns the root node of the FS
func (cfs *FS) Root() (fs.Node, error) {
	// Check if there's a cached root
	if cfs.root != nil {
		return cfs.root, nil
	}

	cfs.counters.register("open")
	cfs.counters.register("open-ro")
	cfs.counters.register("open-ro-error")
	cfs.counters.register("open-rw")
	cfs.counters.register("open-rw-error")

	// Create a dummy dir that will be our root ref
	cfs.root = &fs.Tree{}
	ftRoot, err := cfs.FTRoot()
	if err != nil {
		return nil, err
	}
	cfs.root.Add("current", ftRoot)

	// TODO(tsileo): add "tag" to snapshot instead of message, and `Mkdir tags/mytag` to create a new tags
	cfs.root.Add("tags", &fs.Tree{})

	// TODO(tsileo): list all verisons YYYY-MM-DDTHH:MM:SS
	cfs.root.Add("versions", &fs.Tree{})

	// TODO(tsileo): magic "at/2018/myfile", "at/now" to get current RO snapshot
	cfs.root.Add("at", &fs.Tree{})

	// TODO(tsileo): last 100 most recently modified files by mtime
	cfs.root.Add("recent", &recentDir{cfs, &cfs.openLogs})

	// Debug VFS mounted a /.stats
	statsTree := &fs.Tree{}
	statsTree.Add("started_at", &dataNode{data: []byte(startedAt.Format(time.RFC3339))})
	statsTree.Add("fds.json", &dataNode{f: func() ([]byte, error) {
		for _, d := range cfs.openedFds {
			if d.OpenedAt == "" {
				d.OpenedAt = d.openedAt.Format(time.RFC3339)
			}
		}
		return json.Marshal(cfs.openedFds)
	}})
	statsTree.Add("fds", &dataNode{f: func() ([]byte, error) {
		var buf bytes.Buffer
		w := tabwriter.NewWriter(&buf, 0, 0, 1, ' ', tabwriter.TabIndent)
		for _, d := range cfs.openedFds {
			fmt.Fprintln(w, fmt.Sprintf("%s\t%d\t%s\t%v\t%s", d.Path, d.PID, d.PName, d.RW, d.openedAt.Format(time.RFC3339)))
		}
		w.Flush()
		return buf.Bytes(), nil
	}})
	statsTree.Add("open_logs", &dataNode{f: func() ([]byte, error) {
		var buf bytes.Buffer
		w := tabwriter.NewWriter(&buf, 0, 0, 1, ' ', tabwriter.TabIndent)
		for _, d := range cfs.openLogs {
			fmt.Fprintln(w, fmt.Sprintf("%s\t%d\t%s\t%v\t%s", d.Path, d.PID, d.PName, d.RW, d.openedAt.Format(time.RFC3339)))
		}
		w.Flush()
		return buf.Bytes(), nil
	}})
	statsTree.Add("open_logs.json", &dataNode{f: func() ([]byte, error) {
		for _, d := range cfs.openLogs {
			if d.OpenedAt == "" {
				d.OpenedAt = d.openedAt.Format(time.RFC3339)
			}
		}
		return json.Marshal(cfs.openLogs)
	}})
	statsTree.Add("counters", cfs.counters.Tree)
	cfs.root.Add(".stats", statsTree)

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

// FTNode lazy-loads the node from BlobStash FileTree API
func (d *dir) FTNode() (*node, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.node != nil {
		return d.node, nil
	}
	n, err := d.fs.getNode(d.path)
	if err != nil {
		return nil, err
	}
	d.node = n
	return n, nil
}

// Attr implements the fs.Node interface
func (d *dir) Attr(ctx context.Context, a *fuse.Attr) error {
	fmt.Printf("Attr %s\n", d.path)
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
	return nil
}

// Special preloading for the root that fetch the root tree with a depth of 2
// (meaning we fetch the directories of the directories inside the root).
// The root will be cached, and the same struct will always be returned.
func (d *dir) preloadFTRoot() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Fetch the root node with a depth=2
	n, err := d.fs.getNodeAsOf(d.path, 2, 0)
	if err != nil {
		return err
	}
	// Cache the node
	d.node = n

	d.children = map[string]fs.Node{}
	for _, child := range d.node.Children {
		// We can set the node directly, and directories will contains children because we asked
		// for a depth=2 when requesting the root dir
		if child.isFile() {
			d.children[child.Name] = &file{
				path:   filepath.Join(d.path, child.Name),
				fs:     d.fs,
				node:   child,
				parent: d,
			}
		} else {
			d.children[child.Name] = &dir{
				path:   filepath.Join(d.path, child.Name),
				fs:     d.fs,
				node:   child,
				parent: d,
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
		if child.isFile() {
			d.children[child.Name] = &file{
				path:   filepath.Join(d.path, child.Name),
				fs:     d.fs,
				node:   child,
				parent: d,
			}
		} else {
			// The node is set to nil for directories because we haven't fetched to children
			d.children[child.Name] = &dir{
				path:   filepath.Join(d.path, child.Name),
				fs:     d.fs,
				node:   nil,
				parent: d,
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
	fmt.Printf("Rename %s %+v\n", d.path, req)
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

	d.mu.Lock()
	delete(d.children, req.OldName)
	d.mu.Unlock()
	if d, ok := n.(*dir); ok {
		d.path = filepath.Join(filepath.Dir(d.path), req.NewName)
		d.node = nil
		if _, err := d.FTNode(); err != nil {
			return err
		}
	} else {
		f := n.(*file)
		f.path = filepath.Join(filepath.Dir(f.path), req.NewName)
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
	fmt.Printf("Create %v %s\n", d, req.Name)

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
	ro bool

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
	n, err := f.fs.getNode(f.path)
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

	fmt.Printf("Attr %v %v %+v\n", f, n, a)
	return nil
}

// Access implements the fs.NodeAccesser interface
func (f *file) Access(ctx context.Context, req *fuse.AccessRequest) error {
	return nil
}

// Setattr implements the fs.NodeSetattrer
func (f *file) Setattr(ctx context.Context, req *fuse.SetattrRequest, res *fuse.SetattrResponse) error {
	n, err := f.FTNode()
	if err != nil {
		return err
	}
	fmt.Printf("Setattr %v node=%v %v %v\n", f.path, n, req.Mtime, req.Mode)
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

		fmt.Printf("Setattr %v %v\n", f, req)
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
	return filereader.NewFile(context.Background(), f.fs.bs, meta, nil), nil
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
	fmt.Printf("Read RW %s\n", f.f.path)
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
	fmt.Printf("Write %s %d %d\n", f.f.path, len(req.Data), req.Offset)
	n, err := f.tmp.WriteAt(req.Data, req.Offset)
	if err != nil {
		return err
	}
	res.Size = n
	return nil
}

// Flush implements the fs.HandleFlusher interface
func (f *rwFileHandle) Flush(ctx context.Context, req *fuse.FlushRequest) error {
	fmt.Printf("Flush %v %+v\n", f.f, f.f)

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

	// Reset the cached FileTree node
	f.f.node = nil
	if _, err := f.f.FTNode(); err != nil {
		return err
	}

	return nil
}

// Release implements the fuse.HandleReleaser interface
func (f *rwFileHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	fmt.Printf("Release %s\n", f.f.path)
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
