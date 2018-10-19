package main

import (
	"context"
	"os"

	"a4.io/blobstash/pkg/asof"
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)

type atDir struct {
	fs *FS
}

func (*atDir) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Mode = os.ModeDir | 0555
	return nil
}

func (a *atDir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	if asof.IsValid(name) {
		asOf, err := asof.ParseAsOf(name)
		if err != nil {
			return nil, err
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

		return root, nil
	}

	return nil, fuse.ENOENT
}

func (a *atDir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	return []fuse.Dirent{}, nil
}
