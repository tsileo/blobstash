package main

// import "a4.io/blobstash/pkg/filetree/fs"

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"mime/multipart"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"

	"a4.io/blobstash/pkg/client/kvstore"
)

var kvs *kvstore.KvStore
var cache = newCache()

func main() {
	// Scans the arg list and sets up flags
	debug := flag.Bool("debug", false, "print debugging messages.")
	flag.Parse()
	if flag.NArg() < 2 {
		fmt.Fprintf(os.Stderr, "usage: %s MOUNTPOINT REF\n", os.Args[0])
		os.Exit(2)
	}

	var err error
	kvopts := kvstore.DefaultOpts().SetHost(os.Getenv("BLOBS_API_HOST"), os.Getenv("BLOBS_API_KEY"))
	kvopts.SnappyCompression = false
	kvs = kvstore.New(kvopts)

	root := NewFileSystem(flag.Arg(1))

	opts := &nodefs.Options{
		AttrTimeout:  300 * time.Second,
		EntryTimeout: 300 * time.Second,
		Debug:        *debug,
	}
	nfs := pathfs.NewPathNodeFs(root, nil)
	state, _, err := nodefs.MountRoot(flag.Arg(0), nfs.Root(), opts)

	if err != nil {
		fmt.Printf("Mount fail: %v\n", err)
		os.Exit(1)
	}

	go state.Serve()
	fmt.Printf("mounted\n")

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
	Name     string  `json:"name"`
	Ref      string  `json:"ref"`
	Size     int     `json:"size"`
	Type     string  `json:"type"`
	Children []*Node `json:"children"`
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
	File *os.File
	node *WritableNode
	lock sync.Mutex
}

func (*loopbackFile) Utimens(a *time.Time, m *time.Time) fuse.Status {
	fmt.Printf("lf OP Utimens\n")
	return fuse.OK
}
func (f *loopbackFile) InnerFile() nodefs.File {
	return nil
}

func (f *loopbackFile) SetInode(n *nodefs.Inode) {
	fmt.Printf("setting inode %+v\n", n)
}

func (f *loopbackFile) String() string {
	return fmt.Sprintf("loopbackFile(%s)", f.File.Name())
}

func (f *loopbackFile) Read(buf []byte, off int64) (res fuse.ReadResult, code fuse.Status) {
	f.lock.Lock()
	// This is not racy by virtue of the kernel properly
	// synchronizing the open/write/close.
	r := fuse.ReadResultFd(f.File.Fd(), off, len(buf))
	f.lock.Unlock()
	return r, fuse.OK
}

func (f *loopbackFile) Write(data []byte, off int64) (uint32, fuse.Status) {
	f.lock.Lock()
	n, err := f.File.WriteAt(data, off)
	f.lock.Unlock()
	return uint32(n), fuse.ToStatus(err)
}

func (f *loopbackFile) Release() {
	f.lock.Lock()
	f.File.Close()
	f.lock.Unlock()
}

func (f *loopbackFile) Flush() fuse.Status {
	fmt.Printf("lf OP Flush\n")
	f.lock.Lock()

	// Since Flush() may be called for each dup'd fd, we don't
	// want to really close the file, we just want to flush. This
	// is achieved by closing a dup'd fd.
	newFd, err := syscall.Dup(int(f.File.Fd()))
	f.lock.Unlock()

	if err != nil {
		return fuse.ToStatus(err)
	}
	err = syscall.Close(newFd)
	return fuse.ToStatus(err)
}

func (f *loopbackFile) Fsync(flags int) (code fuse.Status) {
	fmt.Printf("lf OP sync\n")
	f.lock.Lock()
	r := fuse.ToStatus(syscall.Fsync(int(f.File.Fd())))
	f.lock.Unlock()

	return r
}

func (f *loopbackFile) Flock(flags int) fuse.Status {
	f.lock.Lock()
	r := fuse.ToStatus(syscall.Flock(int(f.File.Fd()), flags))
	f.lock.Unlock()

	return r
}

func (f *loopbackFile) Truncate(size uint64) fuse.Status {
	fmt.Printf("truncate(%d)\n", size)
	return fuse.OK
	f.lock.Lock()
	r := fuse.ToStatus(syscall.Ftruncate(int(f.File.Fd()), int64(size)))
	f.lock.Unlock()

	return r
}

func (f *loopbackFile) Chmod(mode uint32) fuse.Status {
	f.lock.Lock()
	r := fuse.ToStatus(f.File.Chmod(os.FileMode(mode)))
	f.lock.Unlock()

	return r
}

func (f *loopbackFile) Chown(uid uint32, gid uint32) fuse.Status {
	f.lock.Lock()
	r := fuse.ToStatus(f.File.Chown(int(uid), int(gid)))
	f.lock.Unlock()

	return r
}

func (f *loopbackFile) GetAttr(a *fuse.Attr) fuse.Status {
	fmt.Printf("lf OP Attr\n")
	st := syscall.Stat_t{}
	f.lock.Lock()
	err := syscall.Fstat(int(f.File.Fd()), &st)
	f.lock.Unlock()
	if err != nil {
		return fuse.ToStatus(err)
	}
	a.FromStat(&st)
	fmt.Printf("attr=%+v\n", a)
	return fuse.OK
}

func (f *loopbackFile) Flush2() fuse.Status {
	// if status := f.File.Flush(); status != fuse.OK {
	// return status
	// }
	return fuse.OK

	fmt.Printf("custom flush")

	bodyBuf := &bytes.Buffer{}
	bodyWriter := multipart.NewWriter(bodyBuf)
	fileWriter, err := bodyWriter.CreateFormFile("file", f.node.node.Name)
	if err != nil {
		return fuse.EIO
	}
	contentType := bodyWriter.FormDataContentType()
	bodyWriter.Close()

	fi, err := os.Open(f.node.tmpPath)
	if err != nil {
		return fuse.EIO
	}
	defer fi.Close()
	if _, err := io.Copy(fileWriter, fi); err != nil {
		return fuse.EIO
	}

	resp, err := kvs.Client().DoReq("POST", "/api/filetree/fs/fs/lol/"+f.node.path, map[string]string{
		"Content-Type": contentType,
	}, bodyBuf)
	if err != nil || resp.StatusCode == 500 { // TODO check for the right status code instead
		return fuse.EIO
	}

	// FIXME(tsileo): read the JSON and upload the Node in nodeIndex

	return fuse.OK
}
func (*loopbackFile) Allocate(off uint64, size uint64, mode uint32) (code fuse.Status) {
	return fuse.OK
}

func newLoopbackFile(f *os.File, n *WritableNode) nodefs.File {
	return &loopbackFile{
		File: f,
		node: n,
	}
}

type WritableNode struct {
	node    *Node
	path    string
	tmpPath string
}

type Cache struct {
	openedFiles map[string]*WritableNode
	nodeIndex   map[string]*Node
	mu          sync.Mutex
}

func newCache() *Cache {
	return &Cache{
		openedFiles: map[string]*WritableNode{},
		nodeIndex:   map[string]*Node{},
	}
}

func (c *Cache) newWritableNode(ref, path string) (nodefs.File, error) {
	fmt.Printf("writableNode(%s, %s)\n", ref, path)
	// FIXME(tsileo): use a custom cache dir
	// tmpfile, err := ioutil.TempFile("", "blobfs_node")
	f1, err := ioutil.TempFile("", "blobfs_node")
	if err != nil {
		return nil, err
	}

	var n *Node

	// if ref != "" {
	n, err = c.getNode(ref, path)
	if err != nil {
		return nil, err
	}
	if err := n.Copy(f1); err != nil {
		return nil, err
	}
	f1.Seek(0, os.SEEK_SET)
	if err := f1.Sync(); err != nil {
		return nil, err
	}
	// }

	c.mu.Lock()
	defer c.mu.Unlock()
	wnode := &WritableNode{
		node:    n,
		path:    path,
		tmpPath: f1.Name(),
	}
	c.openedFiles[path] = wnode

	return newLoopbackFile(f1, wnode), nil
}

func (c *Cache) getNode(ref, path string) (*Node, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// XXX(tsileo): HEAD requests to check if the ref at path is the same
	if n, ok := c.nodeIndex[path]; ok {
		return n, nil
	}
	node := &Node{}
	if err := kvs.Client().GetJSON("/api/filetree/fs/fs/lol/"+path, nil, &node); err != nil {
		return nil, err
	}
	c.nodeIndex[path] = node
	return node, nil

}

var nodeIndex = map[string]*Node{}

func getNode(ref, path string) (*Node, error) {
	if n, ok := nodeIndex[path]; ok {
		return n, nil
	}
	node := &Node{}
	// if err := kvs.Client().GetJSON("/api/filetree/fs/ref/"+ref+"/"+path, nil, &node); err != nil {
	if err := kvs.Client().GetJSON("/api/filetree/fs/fs/lol/"+path, nil, &node); err != nil {
		return nil, err
	}
	nodeIndex[path] = node
	return node, nil
}

type FileSystem struct {
	ref string
}

func NewFileSystem(ref string) pathfs.FileSystem {
	return &FileSystem{ref}
}

func (fs *FileSystem) String() string {
	return fmt.Sprintf("FileSystem()")
}

func (fs *FileSystem) SetDebug(_ bool) {}

func (fs *FileSystem) StatFs(name string) *fuse.StatfsOut {
	// out := &fuse.StatfsOut{}
	return nil
}

func (fs *FileSystem) OnMount(nodeFs *pathfs.PathNodeFs) {
}

func (fs *FileSystem) OnUnmount() {}

func (fs *FileSystem) Utimens(path string, a *time.Time, m *time.Time, context *fuse.Context) fuse.Status {
	return fuse.EPERM
}

func (fs *FileSystem) GetAttr(name string, context *fuse.Context) (a *fuse.Attr, code fuse.Status) {
	fmt.Printf("Getattr(%s)\n", name)
	node, err := cache.getNode(fs.ref, name)
	fmt.Printf("node=%+v\n", node)
	if err != nil {
		return nil, fuse.ENOENT
	}
	if node.Type == "dir" {
		return &fuse.Attr{
			Mode: fuse.S_IFDIR | 0755,
		}, fuse.OK
	}
	fmt.Printf("returning size:%d\n", node.Size)
	return &fuse.Attr{
		Mode: fuse.S_IFREG | 0644,
		Size: uint64(node.Size),
	}, fuse.OK
}

func (fs *FileSystem) SetAttr(input *fuse.SetAttrIn, out *fuse.AttrOut) (code fuse.Status) {
	fmt.Printf("SET ATTR\n")
	return fuse.EPERM
}

func (fs *FileSystem) OpenDir(name string, context *fuse.Context) (stream []fuse.DirEntry, status fuse.Status) {
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
	fmt.Printf("Open write=%v\n", flags&fuse.O_ANYWRITE != 0)
	if flags&fuse.O_ANYWRITE != 0 {
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
	return NewFile(node), fuse.OK
}

func (fs *FileSystem) Chmod(path string, mode uint32, context *fuse.Context) (code fuse.Status) {
	return fuse.EPERM
}

func (fs *FileSystem) Chown(path string, uid uint32, gid uint32, context *fuse.Context) (code fuse.Status) {
	return fuse.EPERM
}

func (fs *FileSystem) Truncate(path string, offset uint64, context *fuse.Context) (code fuse.Status) {
	return fuse.EPERM
}

func (fs *FileSystem) Readlink(name string, context *fuse.Context) (out string, code fuse.Status) {
	return "", fuse.EPERM
}

func (fs *FileSystem) Mknod(name string, mode uint32, dev uint32, context *fuse.Context) (code fuse.Status) {
	return fuse.EPERM
}

func (fs *FileSystem) Mkdir(path string, mode uint32, context *fuse.Context) (code fuse.Status) {
	return fuse.EPERM
}

// Don't use os.Remove, it removes twice (unlink followed by rmdir).
func (fs *FileSystem) Unlink(name string, context *fuse.Context) (code fuse.Status) {
	return fuse.EPERM
}

func (fs *FileSystem) Rmdir(name string, context *fuse.Context) (code fuse.Status) {
	return fuse.EPERM
}

func (fs *FileSystem) Symlink(pointedTo string, linkName string, context *fuse.Context) (code fuse.Status) {
	return fuse.EPERM
}

func (fs *FileSystem) Rename(oldPath string, newPath string, context *fuse.Context) (codee fuse.Status) {
	return fuse.EPERM
}

func (fs *FileSystem) Link(orig string, newName string, context *fuse.Context) (code fuse.Status) {
	return fuse.EPERM
}

func (fs *FileSystem) Access(name string, mode uint32, context *fuse.Context) (code fuse.Status) {
	// TODO(tsileo): implement this
	return fuse.OK
}

func (fs *FileSystem) Create(path string, flags uint32, mode uint32, context *fuse.Context) (fuseFile nodefs.File, code fuse.Status) {
	return nil, fuse.EPERM
}

func (fs *FileSystem) GetXAttr(name string, attr string, context *fuse.Context) ([]byte, fuse.Status) {
	return []byte(""), fuse.OK
}

func (fs *FileSystem) SetXAttr(name string, attr string, data []byte, flags int, context *fuse.Context) fuse.Status {
	return fuse.EPERM
}

func (fs *FileSystem) ListXAttr(name string, context *fuse.Context) ([]string, fuse.Status) {
	out := []string{}
	return out, fuse.OK
}

func (fs *FileSystem) RemoveXAttr(name string, attr string, context *fuse.Context) fuse.Status {
	return fuse.EPERM
}

func NewFile(node *Node) *File {
	fmt.Printf("NewFile(%+v)\n", node)
	return &File{
		fd:   uintptr(rand.Uint32()),
		node: node,
	}
}

type File struct {
	fd   uintptr
	node *Node
	nodefs.File
	inode *nodefs.Inode
}

func (f *File) SetInode(inode *nodefs.Inode) {
	f.inode = inode
}

func (f *File) String() string {
	return fmt.Sprintf("File(%s, %s)", f.node.Name, f.node.Ref)
}

func (f *File) Write(data []byte, off int64) (uint32, fuse.Status) {
	return 0, fuse.EPERM
}

func (f *File) Read(buf []byte, off int64) (res fuse.ReadResult, code fuse.Status) {
	data, code := f.node.ReadAt(buf, off)
	return fuse.ReadResultData(data), code
}

func (f *File) Release() {}

func (f *File) Flush() fuse.Status {
	return fuse.OK
}

func (f *File) GetAttr(a *fuse.Attr) fuse.Status {
	a.Mode = fuse.S_IFREG | 0644
	a.Size = uint64(f.node.Size)
	return fuse.OK
}
