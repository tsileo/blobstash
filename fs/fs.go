package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
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
	Root *Dir
}

func NewFS() (fs *FS) {
	fs = &FS{Root: NewDir()}
	return
}

func (fs *FS) Root() (fs.Node, fuse.Error) {
	return fs.Root, nil
}

type Node struct {
	Name string
	Mode os.FileMode
	Ref string
	Size uint64
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
	Children map[string]fs.Node
}

func NewDir() (d *Dir) {
	d = &Dir{}
	d.Node = Node{}
	d.Mode = os.ModeDir
	d.Children = make(map[string]fs.Node)
	return
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
	return out, nil
}

type File struct {
	Node
}

func (f *File) ReadAll(intr fs.Intr) (out []byte, ferr fuse.Error) {
	return []byte(""), ferr
}
