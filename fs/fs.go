/*

Implements a FUSE filesystem, with a focus on snapshots.

The root directory contains two specials directory:

- **latest**, it contains the latest version of each files/directories (e.g. /datadb/mnt/latest/writing).
- **snapshots**, it contains a list of directory with the file/dir name, and inside this directory,
a list of directory: one directory per snapshots, and finally inside this dir,
the file/dir (e.g /datadb/mnt/snapshots/writing/2014-05-04T17:42:48+02:00/writing).

*/
package fs

import (
	"log"
	"io"
	"os"
	"os/signal"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"

	"github.com/tsileo/datadatabase/client"
)

// Mount the filesystem to the given mountpoint
func Mount(mountpoint string) {
	c, err := fuse.Mount(mountpoint)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()
	log.Printf("Mounting read-only filesystem on %v\nCtrl+C to unmount.", mountpoint)
	
	cs := make(chan os.Signal, 1)
	signal.Notify(cs, os.Interrupt)
	go func() {
		for _ = range cs {
			log.Printf("Unmounting %v...\n", mountpoint)
			err := fuse.Unmount(mountpoint)
			if err != nil {
				log.Printf("Error unmounting: %v", err)
			}
			defer os.Exit(0)
		}
	}()

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
	Client *client.Client
}


func NewFS() (fs *FS) {
	client, _ := client.NewClient()
	fs = &FS{Client: client}
	return
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
	for _, meta := range d.fs.Client.Dirs.Get(d.Ref).([]*client.Meta) {
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
			meta := d.fs.Client.Metas.Get(backup.Ref).(*client.Meta)
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
			meta := d.fs.Client.Metas.Get(backup.Ref).(*client.Meta)
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
		//meta, _ := client.NewMetaFromDB(d.fs.Client.Pool, d.Ref)
		meta := d.fs.Client.Metas.Get(d.Ref).(*client.Meta)
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
	FakeFile *client.FakeFile
}

func NewFile(fs *FS, name, ref string, size int) *File {
	f := &File{}
	f.Name = name
	f.Ref = ref
	f.Size = uint64(size)
	f.fs = fs
	f.FakeFile = client.NewFakeFile(f.fs.Client, ref, size)
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
