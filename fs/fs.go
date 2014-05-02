package main

import (
	"flag"
	"fmt"
	"log"
	"io"
	"os"
	"bytes"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"

	"github.com/tsileo/datadatabase/lru"
	"github.com/tsileo/datadatabase/models"
	"github.com/garyburd/redigo/redis"
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
	Client *models.Client
	blobs models.BlobFetcher
}

func NewFS() (fs *FS) {
	client, _ := models.NewClient()
	fs = &FS{Client: client}
	fs.blobs = lru.New(fs.FetchBlob, 512)
	return
}

func (fs *FS) FetchBlob(hash string) []byte {
	con := fs.Client.Pool.Get()
	defer con.Close()
	var buf bytes.Buffer
	data, err := redis.String(con.Do("BGET", hash))
	if err != nil {
		panic("Error FetchBlob")
	}
	buf.WriteString(data)
	return buf.Bytes()
}

func (fs *FS) Root() (fs.Node, fuse.Error) {
	return NewRootDir(fs), nil
}

type Node struct {
	Name string
	Mode os.FileMode
	Ref string
	Size uint64
	fs *FS
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

func NewDir(cfs *FS, name, ref string) (d *Dir) {
	d = &Dir{}
	d.Node = Node{}
	d.Mode = os.ModeDir
	d.fs = cfs
	d.Ref = ref
	d.Name = name
	d.Children = make(map[string]fs.Node)
	return
}

func (d *Dir) readDir() (out []fuse.Dirent, ferr fuse.Error) {
	con := d.fs.Client.Pool.Get()
	defer con.Close()
	members, _ := redis.Strings(con.Do("SMEMBERS", d.Ref))
	for _, member := range members {
		meta, _ := models.NewMetaFromDB(d.fs.Client.Pool, member)
		var dirent fuse.Dirent
		if meta.Type == "file" {
			dirent = fuse.Dirent{Name: meta.Name, Type: fuse.DT_File}
			d.Children[meta.Name] = NewFile(d.fs, meta.Name, meta.Hash, meta.Size)
		} else {
			dirent = fuse.Dirent{Name: meta.Name, Type: fuse.DT_Dir}
			d.Children[meta.Name] = NewDir(d.fs, meta.Name, meta.Hash)
		}
		out = append(out, dirent)
	}
	return
}

func NewRootDir(fs *FS) (d *Dir) {
	d = NewDir(fs, "root", "")
	d.Root = true
	d.fs = fs
	return d
}

func (d *Dir) Lookup(name string, intr fs.Intr) (fs fs.Node, err fuse.Error) {
	fs, ok := d.Children[name]
	if !ok {
		return nil, fuse.ENOENT
	}

	return
}

// TODO(tsileo) NewDirFromMetaList

func (d *Dir) ReadDir(intr fs.Intr) ([]fuse.Dirent, fuse.Error) {
	var out []fuse.Dirent
	if d.Root {
		backups, _ := d.fs.Client.List()
		// Reset the children, backups may have been removed
		d.Children = make(map[string]fs.Node)
		for _, backup := range backups {
			meta, _ := backup.Meta(d.fs.Client.Pool)
			dirent := fuse.Dirent{Name: meta.Name, Type: fuse.DT_Dir}
			if meta.Type == "file" {
				dirent.Type = fuse.DT_File
				d.Children[meta.Name] = NewFile(d.fs, meta.Name, meta.Hash, meta.Size) 
			} else {
				dirent.Type = fuse.DT_Dir
				d.Children[meta.Name] = NewDir(d.fs, meta.Name, meta.Hash)
			}
			out = append(out, dirent)
		}
		return out, nil
	} else {
		return d.readDir()
	}
	return out, nil
}

type File struct {
	Node
	FakeFile *models.FakeFile
}

func NewFile(fs *FS, name, ref string, size int) *File {
	f := &File{}
	f.Name = name
	f.Ref = ref
	f.Size = uint64(size)
	f.fs = fs
	f.FakeFile = models.NewFakeFileWithBlobFetcher(f.fs.Client.Pool, ref, size, fs.blobs)
	return f
}

// TODO(tsileo) handle release request and close FakeFile

func (f *File) Attr() fuse.Attr {
	return fuse.Attr{Inode: 2, Mode: 0444, Size:f.Size}
}

func (f *File) Read(req *fuse.ReadRequest, res *fuse.ReadResponse, intr fs.Intr) fuse.Error {
	if req.Offset >= int64(f.Size) {
		return nil
	}
	buf := make([]byte, req.Size)
	n, err := f.FakeFile.ReadAt(buf, req.Offset)
	if err == io.EOF {
		err = nil
	}
	if err != nil {
		log.Printf("Error reading FakeFile on %v at %d: %v", f.Ref, req.Offset, err)
		return fuse.EIO
	}
	res.Data = buf[:n]
	return nil
}
