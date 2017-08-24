package main

import (
	"context"
	_ "encoding/json"
	"flag"
	"fmt"
	_ "io/ioutil"
	"os"
	_ "path/filepath"
	"strings"
	"time"

	"a4.io/blobstash/pkg/client/blobstore"
	"a4.io/blobstash/pkg/client/kvstore"
	"a4.io/blobstash/pkg/filetree/filetreeutil/node"
	"a4.io/blobstash/pkg/filetree/reader"
	"a4.io/blobstash/pkg/filetree/writer"

	"github.com/dustin/go-humanize"
	"github.com/google/subcommands"
	_ "github.com/mitchellh/go-homedir"
	_ "gopkg.in/yaml.v2"
)

func rerr(msg string, a ...interface{}) subcommands.ExitStatus {
	fmt.Printf(msg, a...)
	return subcommands.ExitFailure
}

func rsuccess(msg string, a ...interface{}) subcommands.ExitStatus {
	fmt.Printf(msg, a...)
	return subcommands.ExitSuccess
}

type filetreeLsCmd struct {
	bs      *blobstore.BlobStore
	kvs     *kvstore.KvStore
	showRef bool
}

func (*filetreeLsCmd) Name() string     { return "filetree-ls" }
func (*filetreeLsCmd) Synopsis() string { return "Display recent blobs" }
func (*filetreeLsCmd) Usage() string {
	return `recent :
	Display recent blobs.
`
}

func (l *filetreeLsCmd) SetFlags(f *flag.FlagSet) {
	f.BoolVar(&l.showRef, "show-ref", false, "Output references")
}

type Node struct {
	Name     string  `json:"name"`
	Type     string  `json:"type"`
	Size     int     `json:"size"`
	Mode     uint32  `json:"mode"`
	ModTime  string  `json:"mtime"`
	Hash     string  `json:"ref"`
	Children []*Node `json:"children,omitempty"`

	Meta *node.RawNode `json:"meta"`

	Data   map[string]interface{} `json:"data,omitempty"`
	XAttrs map[string]string      `json:"xattrs,omitempty"`
}

func displayNode(c *Node, showRef bool) {
	var ref string
	if showRef {
		ref = fmt.Sprintf("%s\t", c.Hash)
	}
	if c.Type == "file" {
		ref = fmt.Sprintf("%s%s\t", ref, humanize.Bytes(uint64(c.Size)))
	} else {
		ref = fmt.Sprintf("%s0 B\t", ref)
	}

	t, err := time.Parse(time.RFC3339, c.ModTime)
	if err != nil {
		panic(err)
	}
	name := c.Name
	if c.Type == "dir" {
		name = name + "/"
	}
	fmt.Printf("%s\t%s%s\n", t.Format("2006-01-02  15:04"), ref, name)
}

func (l *filetreeLsCmd) Execute(_ context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	if f.NArg() == 0 {
		nodes := []*Node{}
		if err := l.kvs.Client().GetJSON("/api/filetree/fs/root?prefix=_filetree:root", nil, &nodes); err != nil {
			return rerr("failed to fetch root: %v", err)
		}
		for _, node := range nodes {
			displayNode(node, l.showRef)
		}
	}
	if f.NArg() == 1 {
		data := strings.Split(f.Arg(0), "/")
		path := strings.Join(data[1:], "/")
		// FIXME(tsileo): not store the type in the key anymore
		key, err := l.kvs.Get(fmt.Sprintf("_filetree:root:dir:%s", data[0]), -1)
		n := &Node{}
		if err != nil {
			return rerr("failed to fetch root key \"%s\": %v", data[0], err)
		}
		if err := l.kvs.Client().GetJSON(fmt.Sprintf("/api/filetree/fs/ref/%s/%s", key.Hash, path), nil, n); err != nil {
			return rerr("failed to fetch node: %v", err)
		}
		for _, c := range n.Children {
			displayNode(c, l.showRef)
		}
	}
	// TODO(tsileo): support filetree-ls rootname/subdir using fs/path API
	return subcommands.ExitSuccess
}

type filetreeDownloadCmd struct {
	bs  *blobstore.BlobStore
	kvs *kvstore.KvStore
}

func (*filetreeDownloadCmd) Name() string     { return "filetree-get" }
func (*filetreeDownloadCmd) Synopsis() string { return "Display recent blobs" }
func (*filetreeDownloadCmd) Usage() string {
	return `recent :
	Display recent blobs.
`
}

func (*filetreeDownloadCmd) SetFlags(_ *flag.FlagSet) {}

func (r *filetreeDownloadCmd) Execute(_ context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	data := strings.Split(f.Arg(0), "/")
	path := strings.Join(data[1:], "/")
	// FIXME(tsileo): not store the type in the key anymore
	// FIXME(tsileo): move this into a filetree client
	key, err := r.kvs.Get(fmt.Sprintf("_filetree:root:dir:%s", data[0]), -1)
	n := &Node{}
	if err != nil {
		return rerr("failed to fetch root key \"%s\": %v", data[0], err)
	}
	if err := r.kvs.Client().GetJSON(fmt.Sprintf("/api/filetree/fs/ref/%s/%s", key.Hash, path), nil, n); err != nil {
		return rerr("failed to fetch node: %v", err)
	}

	// since the `Meta` type is used internally, and the `Hash` fields is the blake2b hash of the JSON encoded struct,
	// the Hash is omitted when converted to JSON
	if n.Meta != nil {
		n.Meta.Hash = n.Hash
	}

	downloader := reader.NewDownloader(r.bs)
	fmt.Printf("%+v\n", n)
	fmt.Printf("%+v\n", n.Meta)

	// If no target path is provided, use the filename
	tpath := f.Arg(1)
	if tpath == "" {
		tpath = n.Name
	}

	if err := downloader.Download(context.TODO(), n.Meta, tpath); err != nil {
		return rerr("failed to download: %v", err)
	}

	return subcommands.ExitSuccess
}

type filetreePutCmd struct {
	bs  *blobstore.BlobStore
	kvs *kvstore.KvStore
}

func (*filetreePutCmd) Name() string     { return "filetree-put" }
func (*filetreePutCmd) Synopsis() string { return "Display recent blobs" }
func (*filetreePutCmd) Usage() string {
	return `recent :
	Display recent blobs.
`
}

func (*filetreePutCmd) SetFlags(_ *flag.FlagSet) {}

func (r *filetreePutCmd) Execute(_ context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	finfo, err := os.Stat(f.Arg(0))
	switch {
	case os.IsNotExist(err):
		return rsuccess("path \"%s\" does not exist", f.Arg(0))
	case err == nil:
	default:
		return rerr("failed to stat file: %v", err)
	}
	var m *node.RawNode
	up := writer.NewUploader(r.bs)
	// It's a dir
	if finfo.IsDir() {
		m, err = up.PutDir(f.Arg(0))
		if err != nil {
			return rerr("failed to upload: %v", err)
		}
		// FIXME(tsileo): store a FiletreMeta{Hostname, OS} to display (os icon) (hostname) in the web ui
		// hostname, err := os.Hostname()
		// if err != nil {
		// 	return rerr("failed to get hostname: %v", err)
		// }
		// FIXME(tsileo): a way to set the hostname?
	} else {
		// It's a file
		m, err = up.PutFile(f.Arg(0))
		if err != nil {
			return rerr("failed to upload: %v", err)
		}
	}
	fmt.Printf("meta=%+v", m)
	if _, err := r.kvs.Put(fmt.Sprintf("_filetree:root:%s:%s", m.Type, m.Name), m.Hash, []byte("TODO meta data"), -1); err != nil {
		return rerr("faile to set kv entry: %v", err)
	}

	return subcommands.ExitSuccess
}

func main() {
	// TODO(tsileo) config file with server address and collection name
	opts := blobstore.DefaultOpts().SetHost(os.Getenv("BLOBSTASH_API_HOST"), os.Getenv("BLOBSTASH_API_KEY"))
	opts.SnappyCompression = false
	bs := blobstore.New(opts)
	// col := ds.Col("notes23")
	kvopts := kvstore.DefaultOpts().SetHost(os.Getenv("BLOBSTASH_API_HOST"), os.Getenv("BLOBSTASH_API_KEY"))

	// FIXME(tsileo): have GetJSON support snappy?
	kvopts.SnappyCompression = false

	kvs := kvstore.New(kvopts)

	subcommands.Register(subcommands.HelpCommand(), "")
	subcommands.Register(subcommands.FlagsCommand(), "")
	subcommands.Register(subcommands.CommandsCommand(), "")
	subcommands.Register(&filetreePutCmd{bs: bs, kvs: kvs}, "")
	subcommands.Register(&filetreeDownloadCmd{bs: bs, kvs: kvs}, "")
	subcommands.Register(&filetreeLsCmd{bs: bs, kvs: kvs}, "")

	flag.Parse()
	ctx := context.Background()
	os.Exit(int(subcommands.Execute(ctx)))
}
