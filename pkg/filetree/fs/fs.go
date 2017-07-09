package fs // import "a4.io/blobstash/pkg/filetree/fs"

import (
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"

	"a4.io/blobstash/pkg/client/kvstore"
)

var kvs *kvstore.KvStore

func CLI() {
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

var nodeIndex = map[string]*Node{}

func getNode(ref, path string) (*Node, error) {
	if n, ok := nodeIndex[path]; ok {
		return n, nil
	}
	node := &Node{}
	if err := kvs.Client().GetJSON("/api/filetree/fs/ref/"+ref+"/"+path, nil, &node); err != nil {
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
	node, err := getNode(fs.ref, name)
	if err != nil {
		return nil, fuse.ENOENT
	}
	if node.Type == "dir" {
		return &fuse.Attr{
			Mode: fuse.S_IFDIR | 0755,
		}, fuse.OK
	}
	return &fuse.Attr{
		Mode: fuse.S_IFREG | 0644,
		Size: uint64(node.Size),
	}, fuse.OK
}

func (fs *FileSystem) OpenDir(name string, context *fuse.Context) (stream []fuse.DirEntry, status fuse.Status) {
	node, err := getNode(fs.ref, name)
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
	node, err := getNode(fs.ref, name)
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
	return &File{
		fd:   uintptr(rand.Uint32()),
		node: node,
	}
}

type File struct {
	fd   uintptr
	node *Node
	nodefs.File
}

func (f *File) SetInode(inode *nodefs.Inode) {}

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
