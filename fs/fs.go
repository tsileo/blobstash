package main

import (
	"flag"
	"fmt"
	"log"
	"io"
	"os"
	"bytes"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"

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
}

func NewFS() (fs *FS) {
	client, _ := models.NewClient()
	fs = &FS{Client: client}
	return
}

func (fs *FS) FetchBlob(hash string) interface{} {
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
	Latest bool
	Snapshots bool
	SnapshotDir bool
	FakeDir bool
	FakeDirContent []fuse.Dirent
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
	for _, meta := range d.fs.Client.Dirs.Get(d.Ref).([]*models.Meta) {
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

func NewLatestDir(fs *FS) (d *Dir) {
	d = NewDir(fs, "latest", "")
	d.Latest = true
	d.fs = fs
	return d
}

func NewSnapshotDir(cfs *FS, name, ref string) (d *Dir) {
	d = &Dir{}
	d.Node = Node{}
	d.Mode = os.ModeDir
	d.fs = cfs
	d.Ref = ref
	d.Name = name
	d.Children = make(map[string]fs.Node)
	d.SnapshotDir = true
	return
}

func NewSnapshotsDir(cfs *FS) (d *Dir) {
	d = NewDir(cfs, "snapshots", "")
	d.fs = cfs
	d.Snapshots = true
	return
}

func NewFakeDir(cfs *FS, name, ref string) (d *Dir) {
	d = NewDir(cfs, name, ref)
	d.fs = cfs
	d.Children = make(map[string]fs.Node)
	d.FakeDir = true
	return
}

func (d *Dir) Lookup(name string, intr fs.Intr) (fs fs.Node, err fuse.Error) {
	fs, ok := d.Children[name]
	if !ok {
		return nil, fuse.ENOENT
	}
	return
}

// TODO(tsileo) NewDirFromMetaList

func (d *Dir) ReadDir(intr fs.Intr) (out []fuse.Dirent, err fuse.Error) {
	switch {
	case d.Root:
		d.Children = make(map[string]fs.Node)
		dirent := fuse.Dirent{Name: "latest", Type: fuse.DT_Dir}
		out = append(out, dirent)
		d.Children["latest"] = NewLatestDir(d.fs)
		d.Children["snapshots"] = NewSnapshotsDir(d.fs)
		dirent = fuse.Dirent{Name: "snapshots", Type: fuse.DT_Dir}
		out = append(out, dirent)
		return

	case d.Latest:
		d.Children = make(map[string]fs.Node)
		backups, _ := d.fs.Client.Latest()
		for _, backup := range backups {
			meta := d.fs.Client.Metas.Get(backup.Ref).(*models.Meta)
			//meta, _ := backup.Meta(d.fs.Client.Pool)
			if backup.Type == "file" {
				dirent := fuse.Dirent{Name: meta.Name, Type: fuse.DT_File}
				d.Children[meta.Name] = NewFile(d.fs, meta.Name, meta.Hash, meta.Size)
				out = append(out, dirent)
			} else {
				dirent := fuse.Dirent{Name: meta.Name, Type: fuse.DT_Dir}
				d.Children[meta.Name] = NewDir(d.fs, meta.Name, meta.Hash)
				out = append(out, dirent)
			}
		}
		return out, nil
	
	case d.Snapshots:
		d.Children = make(map[string]fs.Node)
		backups, _ := d.fs.Client.Latest()
		for _, backup := range backups {
			meta := d.fs.Client.Metas.Get(backup.Ref).(*models.Meta)
			//meta, _ := backup.Meta(d.fs.Client.Pool)
			dirent := fuse.Dirent{Name: meta.Name, Type: fuse.DT_Dir}
			d.Children[meta.Name] = NewSnapshotDir(d.fs, meta.Name, meta.Hash)
			out = append(out, dirent)
		}
		return out, nil

	case d.SnapshotDir:
		d.Children = make(map[string]fs.Node)
		indexmetas, _ := d.fs.Client.Snapshots(d.Name)
		for _, im := range indexmetas {
			// TODO the index to dirname => blocked with one Node
			stime := time.Unix(int64(im.Index), 0) 
			sname := stime.Format(time.RFC3339)
			meta := im.Meta
			out = append(out, fuse.Dirent{Name: sname, Type: fuse.DT_Dir})
			d.Children[sname] = NewFakeDir(d.fs, meta.Name, meta.Hash)
		}
		return out, nil

	case d.FakeDir:
		d.Children = make(map[string]fs.Node)
		//meta, _ := models.NewMetaFromDB(d.fs.Client.Pool, d.Ref)
		meta := d.fs.Client.Metas.Get(d.Ref).(*models.Meta)
		if meta.Type == "file" {
			dirent := fuse.Dirent{Name: meta.Name, Type: fuse.DT_File}
			d.Children[meta.Name] = NewFile(d.fs, meta.Name, meta.Hash, meta.Size)
			out = append(out, dirent)
		} else {
			dirent := fuse.Dirent{Name: meta.Name, Type: fuse.DT_Dir}
			d.Children[meta.Name] = NewDir(d.fs, meta.Name, meta.Hash)
			out = append(out, dirent)
		}
		return out, nil
	}
	return d.readDir()
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
	f.FakeFile = models.NewFakeFile(f.fs.Client, ref, size)
	return f
}

// TODO(tsileo) handle release request and close FakeFile if needed?

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
