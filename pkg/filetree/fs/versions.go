package fs

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"a4.io/blobstash/pkg/asof"
	"a4.io/blobstash/pkg/client/clientutil"
	"a4.io/blobstash/pkg/ctxutil"
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)

const vfmt = "2006-01-02T15:04:05"

// versionsDir holds a magic dir that list versions/snapshots
type versionsDir struct {
	fs *FS
}

var _ fs.Node = (*versionsDir)(nil)
var _ fs.HandleReadDirAller = (*versionsDir)(nil)
var _ fs.NodeStringLookuper = (*versionsDir)(nil)

// Attr implements the fs.Node interface
func (*versionsDir) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Mode = os.ModeDir | 0555
	return nil
}

// Lookup implements the fs.NodeStringLookuper interface
func (a *versionsDir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	if asof.IsValid(name) {
		asOf, err := asof.ParseAsOf(name)
		if err != nil {
			return nil, err
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

type version struct {
	Ref       string `json:"ref"`
	CreatedAt int64  `json:"created_at"`
}

type versionsResp struct {
	Versions []*version `json:"versions"`
}

func (v *versionsDir) load() (*versionsResp, error) {
	resp, err := v.fs.clientUtil.Get(
		fmt.Sprintf("/api/filetree/versions/fs/%s", v.fs.ref),
		clientutil.EnableJSON(),                            // XXX  this endpoint does not support msgpack
		clientutil.WithHeader(ctxutil.NamespaceHeader, ""), // Disable the stash/namespace
	)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if err := clientutil.ExpectStatusCode(resp, http.StatusOK); err != nil {
		return nil, err
	}

	res := &versionsResp{}
	if err := clientutil.Unmarshal(resp, res); err != nil {
		return nil, err
	}

	return res, nil
}

// ReadDirAll implements the fs.HandleReadDirAller interface
func (v *versionsDir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	out := []fuse.Dirent{}
	resp, err := v.load()
	if err != nil {
		return nil, err
	}

	for _, v := range resp.Versions {
		out = append(out, fuse.Dirent{Type: fuse.DT_Dir, Name: time.Unix(0, v.CreatedAt).Format(vfmt)})
	}

	return out, nil
}
