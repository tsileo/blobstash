package main

import (
	"context"
	"os"

	"a4.io/blobstash/pkg/asof"
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)

// atDir holds a magic "time travel" directory, by parsingd directory name as "asOf" on the fly
type atDir struct {
	fs *FS
}

var _ fs.Node = (*atDir)(nil)
var _ fs.HandleReadDirAller = (*atDir)(nil)
var _ fs.NodeStringLookuper = (*atDir)(nil)

// Attr implements the fs.Node
func (*atDir) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Mode = os.ModeDir | 0555
	return nil
}

// Lookup implements the fs.NodeStringLookuper
func (a *atDir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	if asof.IsValid(name) {
		asOf, err := asof.ParseAsOf(name)
		if err != nil {
			// TODO(tsileo): log the error
			return nil, fuse.ENOENT
		}

		cachedRoot, ok := a.fs.atCache.Get(asOf)
		if ok {
			return cachedRoot.(*dir), nil
		}

		root := &dir{
			path: "/",
			fs:   a.fs,
			node: nil,
			ro:   true,
			asOf: asOf,
		}

		// Actually loads it
		if err := root.preloadFTRoot(); err != nil {
			return nil, err
		}

		if root.node == nil {
			return nil, fuse.ENOENT
		}

		a.fs.atCache.Add(asOf, root)

		return root, nil
	}

	return nil, fuse.ENOENT
}

// ReadDirAll implements the fs.HandleReadDirAller interface
func (a *atDir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	return []fuse.Dirent{}, nil
}
