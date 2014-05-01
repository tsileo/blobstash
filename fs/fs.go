package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"

	"github.com/tsileo/datadatabase/models"
)

var Usage = func() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s MOUNTPOINT\n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	flag.Usage = Usage
	flag.Parse()

	if flag.NArg() != 1 {
		Usage()
		os.Exit(2)
	}
	mountpoint := flag.Arg(0)

	c, err := fuse.Mount(mountpoint)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	err = fs.Serve(c, NewFS())
	if err != nil {
		log.Fatal(err)
	}

	// check if the mount process has an error to report
	<-c.Ready
	if err := c.MountError; err != nil {
		log.Fatal(err)
	}
}

type FS struct {
	RootDir *Dir
}

func NewFS() (fs *FS) {
	fs = &FS{RootDir: NewRootDir()}
	return
}

func (fs *FS) Root() (fs.Node, fuse.Error) {
	return fs.RootDir, nil
}

type Node struct {
	Name string
	Mode os.FileMode
	Ref string
	Size uint64
	Client *models.Client
}

func (n *Node) Attr() fuse.Attr {
	return fuse.Attr{Mode: n.Mode, Size: n.Size}
}

func (n *Node) Setattr(req *fuse.SetattrRequest, resp *fuse.SetattrResponse, intr fs.Intr) fuse.Error {
	n.Mode = req.Mode
	return nil
}

type Dir struct {
	Node
	Root bool
	Children map[string]fs.Node
}

func NewDir() (d *Dir) {
	d = &Dir{}
	d.Node = Node{}
	d.Mode = os.ModeDir
	d.Client, _ = models.NewClient()
	d.Children = make(map[string]fs.Node)
	return
}

func NewRootDir() (d *Dir) {
	d = NewDir()
	d.Root = true
	return d
}

func (d *Dir) Lookup(name string, intr fs.Intr) (fs fs.Node, err fuse.Error) {
	fs, ok := d.Children[name]
	if !ok {
		return nil, fuse.ENOENT
	}

	return
}

func (d *Dir) ReadDir(intr fs.Intr) ([]fuse.Dirent, fuse.Error) {
	var out []fuse.Dirent
	if d.Root {
		backups, _ := d.Client.List()
		for _, backup := range backups {
			meta, _ := backup.Meta(d.Client.Pool)
			dirent := fuse.Dirent{Name: meta.Name, Type: fuse.DT_Dir}
			if meta.Type == "file" {
				dirent.Type = fuse.DT_File
				d.Children[meta.Name] = NewFile(meta.Name, meta.Hash, meta.Size) 
			}
			out = append(out, dirent)
		}
	}
	log.Printf("root:%+v", out)
	return out, nil
}

type File struct {
	Node
}

func NewFile(name, ref string, size int) *File {
	f := &File{}
	f.Name = name
	f.Ref = ref
	f.Size = uint64(size)
	f.Client, _ = models.NewClient()
	return f
}

func (f *File) Attr() fuse.Attr {
	return fuse.Attr{Inode: 2, Mode: 0444, Size:f.Size}
}

func (f *File) ReadAll(intr fs.Intr) (out []byte, ferr fuse.Error) {
	return []byte(""), ferr
}
