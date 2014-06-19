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
	"io"
	"log"
	"os"
	"os/signal"
	"time"
	"fmt"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"

	"github.com/jinzhu/now"

	"github.com/tsileo/blobstash/client"
)

var blobClient *client.Client

// Mount the filesystem to the given mountpoint
func Mount(mountpoint string, stop <-chan bool, stopped chan<- bool) {
	c, err := fuse.Mount(mountpoint)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()
	log.Printf("Mounting read-only filesystem on %v\nCtrl+C to unmount.", mountpoint)

	cs := make(chan os.Signal, 1)
	signal.Notify(cs, os.Interrupt)
	go func() {
		select {
		case <-cs:
			log.Printf("got signal")
			break
		case <-stop:
			log.Printf("got stop")
			break
		}
		log.Println("Closing client...")
		blobClient.Blobs.Close()
		log.Printf("Unmounting %v...\n", mountpoint)
		err := fuse.Unmount(mountpoint)
		if err != nil {
			log.Printf("Error unmounting: %v", err)
		} else {
			stopped <-true
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
	Client  *client.Client
}

// NewFS initialize a new file system.
func NewFS() (fs *FS) {
	// Override supported time format
	now.TimeFormats = []string{"2006-1-2T15:4:5", "2006-1-2T15:4", "2006-1-2T15", "2006-1-2", "2006-1", "2006"}
	blobClient, _ = client.NewClient("", []string{})
	fs = &FS{Client: blobClient}
	return
}

func (fs *FS) Root() (fs.Node, fuse.Error) {
	return NewRootDir(fs), nil
}

type Node struct {
	Name string
	Mode os.FileMode
	Ref  string
	Size uint64
	ModTime string
	//Mode uint32
	fs   *FS
}

func (n *Node) Attr() fuse.Attr {
	t, _ := time.Parse(time.RFC3339, n.ModTime)
	return fuse.Attr{Mode: n.Mode, Size: n.Size, Mtime: t}
}

func (n *Node) Setattr(req *fuse.SetattrRequest, resp *fuse.SetattrResponse, intr fs.Intr) fuse.Error {
	n.Mode = req.Mode
	return nil
}

type Dir struct {
	Node
	Root           bool
	RootHost       bool
	Latest         bool
	Snapshots      bool
	SnapshotDir    bool
	FakeDir        bool
	AtRoot         bool
	AtDir          bool
	FakeDirContent []fuse.Dirent
	Children       map[string]fs.Node
	Host           string
	SnapKey        string
	Hostname string
}

func NewDir(cfs *FS, name, host, ref string, modTime string, mode os.FileMode) (d *Dir) {
	d = &Dir{}
	d.Hostname = host
	d.Node = Node{}
	d.Mode = os.ModeDir
	d.fs = cfs
	d.Ref = ref
	d.Name = name
	d.ModTime = modTime
	d.Mode = os.FileMode(mode)
	d.Children = make(map[string]fs.Node)
	return
}

func (d *Dir) readDir() (out []fuse.Dirent, ferr fuse.Error) {
	con := d.fs.Client.Pool.Get()
	defer con.Close()
	//log.Printf("fs: readDir %v", d.Ref)
	//d.fs.Client.Dirs.Get(d.Ref).([]*client.Meta)
	//metas, err := d.fs.Client.DirIter(d.Ref)
	//if err != nil {
	//	log.Printf("fs: Error readDir %v", err)
	//}
	for _, meta := range d.fs.Client.Dirs.Get(d.Ref).([]*client.Meta) {
		var dirent fuse.Dirent
		if meta.Type == "file" {
			dirent = fuse.Dirent{Name: meta.Name, Type: fuse.DT_File}
			d.Children[meta.Name] = NewFile(d.fs, meta.Name, d.Hostname, meta.Ref, meta.Size, meta.ModTime, os.FileMode(meta.Mode))
		} else {
			dirent = fuse.Dirent{Name: meta.Name, Type: fuse.DT_Dir}
			d.Children[meta.Name] = NewDir(d.fs, meta.Name, d.Hostname, meta.Ref, meta.ModTime, os.FileMode(meta.Mode))
		}
		out = append(out, dirent)
	}
	return
}

func NewRootDir(fs *FS) (d *Dir) {
	d = NewDir(fs, "root", "", "", "", os.ModeDir)
	d.Root = true
	d.fs = fs
	return d
}

func NewRootHostDir(fs *FS, host string) (d *Dir) {
	d = NewDir(fs, host, "", "", "", os.ModeDir)
	d.RootHost = true
	d.fs = fs
	return d
}

func NewLatestDir(fs *FS, host string) (d *Dir) {
	d = NewDir(fs, "latest", "", "", "", os.ModeDir)
	d.Latest = true
	d.fs = fs
	d.Host = host
	return d
}

func NewAtRootDir(fs *FS, host string) (d *Dir) {
	d = NewDir(fs, "at", "", "", "", os.ModeDir)
	d.AtRoot = true
	d.fs = fs
	d.Host = host
	return d
}

func NewAtDir(cfs *FS, name, ref, snapKey string) (d *Dir) {
	d = &Dir{}
	d.SnapKey = snapKey
	d.Node = Node{}
	d.Mode = os.ModeDir
	d.fs = cfs
	d.Ref = ref
	d.Name = name
	d.Children = make(map[string]fs.Node)
	d.AtDir = true
	return
}

func NewSnapshotsDir(cfs *FS, host string) (d *Dir) {
	d = NewDir(cfs, "snapshots", "", "", "", os.ModeDir)
	d.fs = cfs
	d.Snapshots = true
	d.Host = host
	return
}

func NewSnapshotDir(cfs *FS, name, ref, snapKey string) (d *Dir) {
	d = &Dir{}
	d.Node = Node{}
	d.Mode = os.ModeDir
	d.fs = cfs
	d.Ref = ref
	d.Name = name
	d.SnapKey = snapKey
	d.Children = make(map[string]fs.Node)
	d.SnapshotDir = true
	return
}

func NewFakeDir(cfs *FS, name, host, ref string) (d *Dir) {
	d = NewDir(cfs, name, host, ref, "", os.ModeDir)
	d.fs = cfs
	d.Children = make(map[string]fs.Node)
	d.FakeDir = true
	return
}

func (d *Dir) Lookup(name string, intr fs.Intr) (fs fs.Node, err fuse.Error) {
	fs, ok := d.Children[name]
	if ok {
		return
	}
	if d.AtDir {
		t, err := now.Parse(name)
		if err == nil {
			ts := t.UTC().Unix()
			backup, _ := client.NewBackup(d.fs.Client, d.SnapKey)
			snap, _ := backup.GetAt(ts)
			if snap != nil {
				return NewDir(d.fs, d.Name, snap.Hostname, snap.Ref, d.ModTime, d.Mode), nil
			}
		}
	}
	return nil, fuse.ENOENT
}

// TODO(tsileo) NewDirFromMetaList

func (d *Dir) ReadDir(intr fs.Intr) (out []fuse.Dirent, err fuse.Error) {
	switch {
	case d.Root:
		d.Children = make(map[string]fs.Node)
		hosts, err := d.fs.Client.Hosts()
		if err != nil {
			panic("failed to fetch hosts")
		}
		for _, host := range hosts {
			dirent := fuse.Dirent{Name: host, Type: fuse.DT_Dir}
			out = append(out, dirent)
			d.Children[host] = NewRootHostDir(d.fs, host)
		}
		return out, err
	case d.RootHost:
		d.Children = make(map[string]fs.Node)
		dirent := fuse.Dirent{Name: "latest", Type: fuse.DT_Dir}
		out = append(out, dirent)
		d.Children["latest"] = NewLatestDir(d.fs, d.Name)
		dirent = fuse.Dirent{Name: "snapshots", Type: fuse.DT_Dir}
		out = append(out, dirent)
		d.Children["snapshots"] = NewSnapshotsDir(d.fs, d.Name)
		dirent = fuse.Dirent{Name: "at", Type: fuse.DT_Dir}
		out = append(out, dirent)
		d.Children["at"] = NewAtRootDir(d.fs, d.Name)
		return

	case d.Latest:
		d.Children = make(map[string]fs.Node)
		backups, err := d.fs.Client.Backups(d.Host)
		if err != nil {
			panic(fmt.Errorf("failed to fetch backups list: %v", err))
		}
		for _, backup := range backups {
			log.Printf("backup: %+v", backup)
			snap, err := backup.Last()
			if err != nil {
				panic(fmt.Errorf("error fetching latest snapshot for backup %v", backup.SnapKey))
			}
			meta := d.fs.Client.Metas.Get(snap.Ref).(*client.Meta)
			//meta, _ := backup.Meta(d.fs.Client.Pool)
			if snap.Type == "file" {
				dirent := fuse.Dirent{Name: meta.Name, Type: fuse.DT_File}
				d.Children[meta.Name] = NewFile(d.fs, meta.Name, snap.Hostname, meta.Ref, meta.Size, meta.ModTime, os.FileMode(meta.Mode))
				out = append(out, dirent)
			} else {
				dirent := fuse.Dirent{Name: meta.Name, Type: fuse.DT_Dir}
				d.Children[meta.Name] = NewDir(d.fs, meta.Name, snap.Hostname, meta.Ref, meta.ModTime, os.FileMode(meta.Mode))
				out = append(out, dirent)
			}
		}
		return out, nil

	case d.Snapshots:
		d.Children = make(map[string]fs.Node)
		backups, err := d.fs.Client.Backups(d.Host)
		if err != nil {
			panic(fmt.Errorf("failed to fetch backups list: %v", err))
		}
		for _, backup := range backups {
			snap, err := backup.Last()
			if err != nil {
				panic(fmt.Errorf("error fetching latest snapshot for backup %v", backup.SnapKey))
			}
			meta := d.fs.Client.Metas.Get(snap.Ref).(*client.Meta)
			//meta, _ := backup.Meta(d.fs.Client.Pool)
			dirent := fuse.Dirent{Name: meta.Name, Type: fuse.DT_Dir}
			d.Children[meta.Name] = NewSnapshotDir(d.fs, meta.Name, meta.Ref, backup.SnapKey)
			out = append(out, dirent)
		}
		return out, nil

	case d.AtRoot:
		d.Children = make(map[string]fs.Node)
		backups, err := d.fs.Client.Backups(d.Host)
		if err != nil {
			panic(fmt.Errorf("failed to fetch backups list: %v", err))
		}
		for _, backup := range backups {
			snap, err := backup.Last()
			if err != nil {
				panic(fmt.Errorf("error fetching latest snapshot for backup %v", backup.SnapKey))
			}
			meta := d.fs.Client.Metas.Get(snap.Ref).(*client.Meta)
			//meta, _ := backup.Meta(d.fs.Client.Pool)
			dirent := fuse.Dirent{Name: meta.Name, Type: fuse.DT_Dir}
			d.Children[meta.Name] = NewAtDir(d.fs, meta.Name, meta.Ref, backup.SnapKey)
			out = append(out, dirent)
		}
		return out, nil

	case d.AtDir:
		return []fuse.Dirent{}, nil

	case d.SnapshotDir:
		d.Children = make(map[string]fs.Node)
		//indexmetas, _ := d.fs.Client.Snapshots(d.Name)
		backup, _ := client.NewBackup(d.fs.Client, d.SnapKey)
		snaphots, _ := backup.Snapshots()
		for _, im := range snaphots {
			// TODO the index to dirname => blocked with one Node
			stime := time.Unix(int64(im.Index), 0)
			sname := stime.Format(time.RFC3339)
			meta := d.fs.Client.Metas.Get(im.Snapshot.Ref).(*client.Meta)
			out = append(out, fuse.Dirent{Name: sname, Type: fuse.DT_Dir})
			d.Children[sname] = NewFakeDir(d.fs, meta.Name, im.Snapshot.Hostname, meta.Hash)
		}
		return out, nil

	case d.FakeDir:
		d.Children = make(map[string]fs.Node)
		//meta, _ := client.NewMetaFromDB(d.fs.Client.Pool, d.Ref)
		meta := d.fs.Client.Metas.Get(d.Ref).(*client.Meta)
		if meta.Type == "file" {
			dirent := fuse.Dirent{Name: meta.Name, Type: fuse.DT_File}
			d.Children[meta.Name] = NewFile(d.fs, meta.Name, d.Hostname, meta.Ref, meta.Size, meta.ModTime, os.FileMode(meta.Mode))
			out = append(out, dirent)
		} else {
			dirent := fuse.Dirent{Name: meta.Name, Type: fuse.DT_Dir}
			d.Children[meta.Name] = NewDir(d.fs, meta.Name, d.Hostname, meta.Ref, meta.ModTime, os.FileMode(meta.Mode))
			out = append(out, dirent)
		}
		return out, nil
	}
	return d.readDir()
}

type File struct {
	Node
	Host string
	FakeFile *client.FakeFile
}

func NewFile(fs *FS, name, host, ref string, size int, modTime string, mode os.FileMode) *File {
	f := &File{}
	f.Host = host
	f.Name = name
	f.Ref = ref
	f.Size = uint64(size)
	f.ModTime = modTime
	f.Mode = mode
	f.fs = fs
	return f
}

// TODO(tsileo) handle release request and close FakeFile if needed?

func (f *File) Attr() fuse.Attr {
	return fuse.Attr{Inode: 2, Mode: 0444, Size: f.Size}
}
func (f *File) Open(req *fuse.OpenRequest, res *fuse.OpenResponse, intr fs.Intr) (fs.Handle, fuse.Error) {
	f.FakeFile = client.NewFakeFile(f.fs.Client, f.Host, f.Ref, int(f.Size))
	return f, nil
}
func (f *File) Release(req *fuse.ReleaseRequest, intr fs.Intr) fuse.Error {
	f.FakeFile = nil
	return nil
}
func (f *File) Read(req *fuse.ReadRequest, res *fuse.ReadResponse, intr fs.Intr) fuse.Error {
	//log.Printf("Read %+v", f)
	if req.Offset >= int64(f.Size) {
		return nil
	}
	buf := make([]byte, req.Size)
	n, err := f.FakeFile.ReadAt(buf, req.Offset)
	if err == io.EOF {
		err = nil
	}
	if err != nil {
		log.Printf("Error reading FakeFile %+v on %v at %d: %v", f, f.Ref, req.Offset, err)
		return fuse.EIO
	}
	res.Data = buf[:n]
	return nil
}
