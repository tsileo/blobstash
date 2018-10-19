package main

import (
	"context"
	"os"
	"path/filepath"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)

type recentDir struct {
	fs  *FS
	dat *[]*fdDebug
}

func (r *recentDir) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Mode = os.ModeDir | 0555
	return nil
}

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

func (r *recentDir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	var out []fuse.Dirent
	for _, d := range *r.dat {
		out = append(out, fuse.Dirent{Type: fuse.DT_File, Name: filepath.Base(d.Path)})
	}
	return out, nil
}
