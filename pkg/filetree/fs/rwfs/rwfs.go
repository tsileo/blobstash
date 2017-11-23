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
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
	"github.com/hashicorp/golang-lru"
	"github.com/pkg/xattr"

	bcache "a4.io/blobstash/pkg/cache"
	"a4.io/blobstash/pkg/client/blobstore"
	"a4.io/blobstash/pkg/client/kvstore"
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
	flag.Parse()
	if flag.NArg() < 2 {
		fmt.Fprintf(os.Stderr, "usage: %s MOUNTPOINT REF\n", os.Args[0])
		flag.PrintDefaults()
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
	bs = blobstore.New(kvopts)

	root := NewFileSystem(flag.Arg(1), *debug)

	opts := &nodefs.Options{
		Debug: false, // *debug,
	}
	nfs := pathfs.NewPathNodeFs(root, nil)
	// state, _, err := nodefs.MountRoot(flag.Arg(0), nfs.Root(), opts)

	conn := nodefs.NewFileSystemConnector(nfs.Root(), opts)

	// XXX(tsileo): different options on READ ONLY mode
	mountOpts := fuse.MountOptions{
		// AllowOther: true,
		Options: []string{
			// FIXME(tsileo): no more nolocalcaches and use notify instead for linux
			"allow_root",
			"allow_other",
			"nolocalcaches",
			// "defer_permissions",
			// "noclock",
			//"auto_xattr",
			// "noappledouble",
			// "noapplexattr",
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
	// FIXME(tsileo): use filetree download to use the cache/blobstore
	resp, err := kvs.Client().DoReq(context.TODO(), "GET", "/api/filetree/file/"+n.Ref, nil, nil)
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

type Cache struct {
	nodeIndex    map[string]*Node
	negNodeIndex map[string]struct{}
	mu           sync.Mutex
	path         string
	blobsCache   *bcache.Cache
}

func newCache(path string) (*Cache, error) {
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
		nodeIndex:    map[string]*Node{},
		negNodeIndex: map[string]struct{}{},
		path:         path,
		blobsCache:   blobsCache,
	}, nil
}

func (c *Cache) Stat(ctx context.Context, hash string) (bool, error) {
	// FIXME(tsileo): is a local check needed, when can we skip the remote check?
	// locStat, err := c.blobsCache.Stat(hash)
	// if err != nil {
	// 	return false, err
	// }
	return bs.Stat(ctx, hash)
}

// Get implements the BlobStore interface for filereader.File
func (c *Cache) Put(ctx context.Context, hash string, data []byte) error {
	if err := c.blobsCache.Add(hash, data); err != nil {
	}
	// FIXME(tsileo): add a stat/exist check once the data contexes is implemented
	if err := bs.Put(ctx, hash, data); err != nil {
		return nil
	}
	return nil
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
		// resp, err := kvs.Client().DoReq(ctx, "GET", "/api/blobstore/blob/"+hash, nil, nil)
		// if err != nil {
		// return nil, err
		// }
		// defer resp.Body.Close()
		// data, err = ioutil.ReadAll(resp.Body)
		data, err := bs.Get(ctx, hash)
		if err != nil {
			return nil, err
		}
		if err := c.blobsCache.Add(hash, data); err != nil {
			return nil, err
		}
	}
	return data, nil
}

func (c *Cache) getNode(ref, path string) (*Node, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// if n, ok := c.nodeIndex[path]; ok {
	// return n, nil
	// }
	node := &Node{}
	if err := kvs.Client().GetJSON(context.TODO(), "/api/filetree/fs/fs/"+ref+"/"+path, nil, &node); err != nil {
		if err == clientutil.ErrNotFound {
			return nil, fuse.ENOENT
		}
		return nil, err
	}
	c.nodeIndex[path] = node
	return node, nil

}

type FileSystem struct {
	ref     string
	debug   bool
	rwLayer string
	up      *writer.Uploader
	mu      sync.Mutex
	ro      bool
}

func NewFileSystem(ref string, debug bool) pathfs.FileSystem {
	return &FileSystem{
		ref:     ref,
		debug:   debug,
		rwLayer: "rw",
		up:      writer.NewUploader(cache),
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

func (fs *FileSystem) GetAttr(name string, context *fuse.Context) (a *fuse.Attr, code fuse.Status) {
	mu.Lock()
	defer mu.Unlock()
	if fs.debug {
		log.Printf("OP Getattr %s", name)
	}

	rwExists, err := fs.rwExists(name)
	if err != nil {
		log.Printf("OP GetAttr %s rwExists err=%+v", name, err)
		return nil, fuse.EIO
	}
	if rwExists {
		fullPath := filepath.Join(fs.rwLayer, name)
		var err error = nil
		st := syscall.Stat_t{}
		if name == "" {
			// When GetAttr is called for the toplevel directory, we always want
			// to look through symlinks.
			err = syscall.Stat(fullPath, &st)
		} else {
			err = syscall.Lstat(fullPath, &st)
		}

		if err != nil {
			return nil, fuse.ToStatus(err)
		}
		a = &fuse.Attr{}
		a.FromStat(&st)
		log.Printf("OUT rwlayer=%+v\n", a)
		return a, fuse.OK
	}
	// return nil, fuse.ToStatus(err)

	// if _, ok := cache.negNodeIndex[name]; ok {
	// return nil, fuse.ENOENT
	// }
	// FIXME(tsileo): check err
	node, err := cache.getNode(fs.ref, name)
	fmt.Printf("node=%+v\nerr=%+v", node, err)
	switch err {
	case fuse.ENOENT:
		return nil, err
	case nil:
	default:
		return nil, fuse.EIO
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
			Atime: node.Mtime(),
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
		Atime: node.Mtime(),
		Ctime: node.Mtime(),
		Mtime: node.Mtime(),
		Owner: *owner,
	}
	log.Printf("OUT=%+v\n", out)
	return out, fuse.OK
}

// func (fs *FileSystem) SetAttr(input *fuse.SetAttrIn, out *fuse.AttrOut) (code fuse.Status) {
// log.Printf("OP SetAttr %+v %+v", input, out)
// return fuse.EPERM
// }

func (fs *FileSystem) openRwDir(name string) (map[string]fuse.DirEntry, error) {
	f, err := os.Open(filepath.Join(fs.rwLayer, name))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
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
				log.Printf("ReadDir entry %q for %q has no stat info", n, name)
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
	return output, nil
}

func (fs *FileSystem) OpenDir(name string, context *fuse.Context) (stream []fuse.DirEntry, status fuse.Status) {
	if fs.debug {
		log.Printf("OP OpenDir %s", name)
	}

	var err error
	var index map[string]fuse.DirEntry
	if !fs.ro {
		// The real directory is the 1st layer, if a file exists locally as a file it will show up instead of the remote version
		index, err = fs.openRwDir(name)
		if err != nil {
			log.Printf("rw layer failed=%+v\n", err)
			return nil, fuse.EIO
		}
		log.Printf("OP OpenDir %s index=%+v", name, index)
	}
	if index == nil {
		index = map[string]fuse.DirEntry{}
	}

	// Now take a look a the remote node
	node, err := cache.getNode(fs.ref, name)
	switch err {
	case fuse.ENOENT:
		return nil, err
	case nil:
	default:
		return nil, fuse.EIO
	}

	log.Printf("OP OpenDir %s remote node=%+v", name, node)
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
				// if child.IsFile() {
				// 	output = append(output, fuse.DirEntry{Name: child.Name, Mode: fuse.S_IFREG})
				// } else {
				// 	output = append(output, fuse.DirEntry{Name: child.Name, Mode: fuse.S_IFDIR})
				// }
			}
		}
	}
	output := []fuse.DirEntry{}
	for _, dirEntry := range index {
		output = append(output, dirEntry)
	}
	return output, fuse.OK
}

func (fs *FileSystem) rwExists(name string) (bool, error) {
	if _, err := os.Stat(filepath.Join(fs.rwLayer, name)); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (fs *FileSystem) Open(name string, flags uint32, context *fuse.Context) (fuseFile nodefs.File, status fuse.Status) {
	if fs.debug {
		log.Printf("OP Open %s write=%v\n", name, flags&fuse.O_ANYWRITE != 0)
	}
	node, err := cache.getNode(fs.ref, name)
	switch err {
	case fuse.ENOENT:
		return nil, err
	case nil:
	default:
		return nil, fuse.EIO
	}

	rwExists, err := fs.rwExists(name)
	if err != nil {
		return nil, fuse.EIO
	}

	if flags&fuse.O_ANYWRITE != 0 || rwExists {
		if flags&fuse.O_ANYWRITE != 0 && fs.ro {
			// XXX(tsileo): is EROFS the right error code
			return nil, fuse.EROFS
		}
		f, err := NewRWFile(fs, name, flags, 0, node)
		return f, fuse.ToStatus(err)

		// 		fh, err := os.OpenFile(filepath.Join(fs.rwLayer, name), int(flags), 0)
		// 		if err != nil {
		// 			return nil, fuse.ToStatus(err)
		// 		}
		// 		if node != nil && os.IsNotExist(err) {
		// 			fullPath := filepath.Join(fs.rwLayer, name)
		// 			if _, err := os.Stat(filepath.Dir(fullPath)); err != nil {
		// 				if os.IsNotExist(err) {
		// 					if err := os.MkdirAll(filepath.Dir(fullPath), 0744); err != nil {
		// 						return nil, fuse.EIO
		// 					}
		// 				} else {
		// 					return nil, fuse.EIO
		// 				}
		// 			}
		// 			fh, err = os.OpenFile(filepath.Join(fs.rwLayer, name), int(flags)|os.O_CREATE, os.FileMode(0644))
		// 			if err != nil {
		// 				return nil, fuse.EIO
		// 			}

		// 			tmtime := time.Unix(int64(node.Mtime()), 0)
		// 			if err := os.Chtimes(filepath.Join(fs.rwLayer, name), tmtime, tmtime); err != nil {
		// 				return nil, fuse.EIO
		// 			}
		// 			if err := node.Copy(fh); err != nil {
		// 				return nil, fuse.EIO
		// 			}
		// 			if err := fh.Sync(); err != nil {
		// 				return nil, fuse.EIO
		// 			}
		// 			if _, err := fh.Seek(0, os.SEEK_SET); err != nil {
		// 				return nil, fuse.EIO
		// 			}
		// 		}
		// 		return nodefs.NewLoopbackFile(fh), fuse.OK
	}
	if node == nil {
		return nil, fuse.ENOENT
	}
	// f, err := NewFile(fs, name, node)
	// if err != nil {
	// 	return nil, fuse.EIO
	// }

	// node, err := cache.getNode(fs.ref, name)
	// if err != nil {
	// return nil, fuse.ENOENT
	// }
	f, err := NewFile(fs, name, node)
	if err != nil {
		return nil, fuse.EIO
	}

	return f, fuse.OK
}

func (fs *FileSystem) Chmod(path string, mode uint32, context *fuse.Context) (code fuse.Status) {
	if fs.debug {
		log.Printf("OP Chmod %s", path)
	}
	return fuse.EPERM
}

func (fs *FileSystem) Chown(path string, uid uint32, gid uint32, context *fuse.Context) (code fuse.Status) {
	if fs.debug {
		log.Printf("OP Chown %s", path)
	}
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
	resp, err := kvs.Client().DoReq(context.TODO(), "PATCH", "/api/filetree/fs/fs/"+fs.ref+"/"+d, nil, bytes.NewReader(js))
	if err != nil || resp.StatusCode != 200 {
		return fuse.EIO
	}

	return fuse.ToStatus(os.Mkdir(filepath.Join(fs.rwLayer, path), os.FileMode(mode)))
	// return fuse.EPERM
}

// Don't use os.Remove, it removes twice (unlink followed by rmdir).
func (fs *FileSystem) Unlink(name string, _ *fuse.Context) (code fuse.Status) {
	if fs.debug {
		log.Printf("OP Unlink %s", name)
	}
	if fs.ro {
		return fuse.EPERM
	}
	rwExists, err := fs.rwExists(name)
	if err != nil {
		return fuse.EIO
	}
	if rwExists {
		if err := fuse.ToStatus(syscall.Unlink(filepath.Join(fs.rwLayer, name))); err != fuse.OK && err != fuse.ENOENT {
			// XXX(tsileo): what about ENOENT here, and no ENOENT on remote, should this be a special error?
			return err
		}
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
	if fs.debug {
		log.Printf("OP Rmdir %s", name)
	}
	if fs.ro {
		return fuse.EPERM
	}

	rwExists, err := fs.rwExists(name)
	if err != nil {
		return fuse.EIO
	}
	if rwExists {
		// XXX(tsileo): like Unlink OP, we should check the local error `lerr`
		if err := fuse.ToStatus(syscall.Rmdir(filepath.Join(fs.rwLayer, name))); err != fuse.ENOENT && err != fuse.OK {
			return err
		}
	}

	node, err := cache.getNode(fs.ref, name)
	switch err {
	case fuse.ENOENT:
		return nil, err
	case nil:
	default:
		return nil, fuse.EIO
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
	if fs.debug {
		log.Printf("OP Rename %s %s", oldPath, newPath)
	}
	if fs.ro {
		return fuse.EPERM
	}
	rwExists, err := fs.rwExists(oldPath)
	if err != nil {
		return fuse.EIO
	}
	if rwExists {
		return fuse.ToStatus(os.Rename(filepath.Join(fs.rwLayer, oldPath), filepath.Join(fs.rwLayer, newPath)))
	}

	node, err := cache.getNode(fs.ref, oldPath)
	if err != nil {
		return fuse.EIO
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
	if fs.debug {
		log.Printf("OP Access %s", name)
	}
	return fuse.OK
}

func (fs *FileSystem) Create(path string, flags uint32, mode uint32, context *fuse.Context) (fuseFile nodefs.File, code fuse.Status) {
	if fs.debug {
		log.Printf("OP Create %s", path)
	}
	f, err := NewRWFile(fs, path, flags, mode, nil)
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

	rwExists, err := fs.rwExists(name)
	if err != nil {
		return nil, fuse.EIO
	}
	if rwExists {
		if list, err := xattr.List(filepath.Join(fs.rwLayer, name)); err == nil {
			return list, fuse.OK
		}
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
	return fuse.OK
}

type RWFile struct {
	nodefs.File
	node    *Node
	path    string
	oldPath string // FIXME(tsileo): set it on remove
	fs      *FileSystem
}

func NewRWFile(fs *FileSystem, path string, flags, mode uint32, node *Node) (*RWFile, error) {
	fullPath := filepath.Join(fs.rwLayer, path)

	fh, err := os.OpenFile(fullPath, int(flags), os.FileMode(mode))
	if err != nil {
		return nil, err
	}
	if os.IsNotExist(err) {
		if _, err := os.Stat(filepath.Dir(fullPath)); err != nil {
			if os.IsNotExist(err) {
				if err := os.MkdirAll(filepath.Dir(fullPath), 0744); err != nil {
					return nil, err
				}
			} else {
				return nil, err
			}
		}
		fh, err = os.OpenFile(fullPath, int(flags)|os.O_CREATE, os.FileMode(mode))
		if err != nil {
			return nil, err
		}
		if node != nil {
			tmtime := time.Unix(int64(node.Mtime()), 0)
			if err := os.Chtimes(fullPath, tmtime, tmtime); err != nil {
				return nil, err
			}
			if err := node.Copy(fh); err != nil {
				return nil, err
			}
			if err := fh.Sync(); err != nil {
				return nil, err
			}
			if _, err := fh.Seek(0, os.SEEK_SET); err != nil {
				return nil, err
			}
		}
	}
	lf := nodefs.NewLoopbackFile(fh)
	return &RWFile{
		fs:   fs,
		path: path,
		node: node,
		File: lf,
	}, nil
}

func (f *RWFile) Flush() fuse.Status {
	if f.fs.debug {
		log.Printf("OP Flush %+s", f)
	}
	fullPath := filepath.Join(f.fs.rwLayer, f.path)
	rawNode, err := f.fs.up.PutFile(fullPath)
	if err != nil {
		log.Printf("failed to upload: %v", err)
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
		log.Printf("upload failed with resp %+v/%+v", resp, err)
		return fuse.EIO
	}
	return f.File.Flush()
}

func (f *RWFile) Release() {
	if f.fs.debug {
		log.Printf("OP Release %+s", f)
	}
	// FIXME(tsileo): remove file on rwLayer
	f.File.Release()
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
	a.Ctime = f.node.Mtime()
	a.Atime = f.node.Mtime()
	a.Mtime = f.node.Mtime()
	a.Owner = *owner
	return fuse.OK
}
