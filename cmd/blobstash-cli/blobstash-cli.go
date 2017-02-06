package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	_ "path/filepath"
	"strings"
	"time"

	"a4.io/blobstash/pkg/client/blobstore"
	"a4.io/blobstash/pkg/client/kvstore"
	"a4.io/blobstash/pkg/filetree/filetreeutil/meta"
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

	Data   map[string]interface{} `json:"data,omitempty"`
	XAttrs map[string]string      `json:"xattrs,omitempty"`
}

func (l *filetreeLsCmd) Execute(_ context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	if f.NArg() == 0 {
		keys, err := l.kvs.Keys("_filetree:root:", "", "", -1)
		if err != nil {
			return rerr("failed to list keys: %v", err)
		}
		// TODO(tsileo): implements keysHandler
		for _, k := range keys {
			// fmt.Printf("k=%+v\n", k)
			// TODO(tsileo): switch kv.Version to int64
			t := time.Unix(0, int64(k.Version))
			data := strings.Split(k.Key, ":")
			// TODO(tsileo): use tabwriter and short hash expand func like the blobs-cli one?
			var ref string
			if l.showRef {
				ref = fmt.Sprintf("%s\t", k.Hash)
			}
			name := data[3]
			if data[2] == "dir" {
				name = name + "/"
			}
			fmt.Printf("%s\t%s%s\n", t.Format("2006-01-02  15:04"), ref, name)
		}
	}
	if f.NArg() == 1 {
		data := strings.Split(f.Arg(0), "/")
		path := strings.Join(data[1:], "/")
		// FIXME(tsileo): not store the type in the key anymore
		key, err := l.kvs.Get(fmt.Sprintf("_filetree:root:dir:%s", data[0]), -1)
		if err != nil {
			return rerr("failed to fetch root key \"%s\": %v", data[0], err)
		}
		resp, err := l.kvs.Client().DoReq("GET", fmt.Sprintf("/api/filetree/fs/ref/%s/%s", key.Hash, path), nil, nil)
		defer resp.Body.Close()
		if err != nil {
			return rerr("API call failed: %v", err)
		}
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return rerr("failed to read request body: %v", err)
		}
		if resp.StatusCode != 200 {
			return rerr("API call failed: %s", body)
		}
		n := &Node{}
		if err := json.Unmarshal(body, n); err != nil {
			return rerr("failed to unmarshal: %v", err)
		}
		for _, c := range n.Children {
			var ref string
			if l.showRef {
				ref = fmt.Sprintf("%s\t", c.Hash)
			}
			if c.Type == "file" {
				ref = fmt.Sprintf("%s%s\t", ref, humanize.Bytes(uint64(c.Size)))
			}
			t, err := time.Parse(time.RFC3339, c.ModTime)
			if err != nil {
				return rerr("failed to parse date: %s", err)
			}
			name := c.Name
			if c.Type == "dir" {
				name = name + "/"
			}
			fmt.Printf("%s\t%s%s\n", t.Format("2006-01-02  15:04"), ref, name)
			// fmt.Printf("%+v\n", c)
		}
	}
	// TODO(tsileo): support filetree-ls rootname/subdir using fs/path API
	return subcommands.ExitSuccess
}

type filetreeDownloadCmd struct {
	bs  *blobstore.BlobStore
	kvs *kvstore.KvStore
}

func (*filetreeDownloadCmd) Name() string     { return "filetree-download" }
func (*filetreeDownloadCmd) Synopsis() string { return "Display recent blobs" }
func (*filetreeDownloadCmd) Usage() string {
	return `recent :
	Display recent blobs.
`
}

func (*filetreeDownloadCmd) SetFlags(_ *flag.FlagSet) {}

func (r *filetreeDownloadCmd) Execute(_ context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
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
	var m *meta.Meta
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
	bs := blobstore.New(opts)
	// col := ds.Col("notes23")
	kvopts := kvstore.DefaultOpts().SetHost(os.Getenv("BLOBSTASH_API_HOST"), os.Getenv("BLOBSTASH_API_KEY"))
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