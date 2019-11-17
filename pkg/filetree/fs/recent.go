package fs

import (
	"context"
	"os"
	"path/filepath"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)

// recentDir implements a magic dir that list the last 100 files recently opened
type recentDir struct {
	fs  *FS
	dat *[]*fdDebug
}

var _ fs.Node = (*recentDir)(nil)
var _ fs.HandleReadDirAller = (*recentDir)(nil)
var _ fs.NodeStringLookuper = (*recentDir)(nil)

// Attr implements the fs.Node interface
func (r *recentDir) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Mode = os.ModeDir | 0555
	return nil
}

// Lookup implements the fs.NodeStringLookuper interface
func (r *recentDir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	for _, d := range *r.dat {
		if filepath.Base(d.Path) == name {
			return &file{
				path:   d.Path,
				fs:     r.fs,
				node:   nil,
				parent: nil,
				ro:     true,
			}, nil
		}
	}

	return nil, fuse.ENOENT
}

// ReadDirAll implements the fs.HandleReadDirAller
func (r *recentDir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	var out []fuse.Dirent
	for _, d := range *r.dat {
		out = append(out, fuse.Dirent{Type: fuse.DT_File, Name: filepath.Base(d.Path)})
	}
	return out, nil
}
