package main

import (
	"flag"
	"fmt"
	"os"

	"a4.io/blobstash/pkg/client/blobstore"
	"a4.io/blobstash/pkg/client/kvstore"
	"a4.io/blobstash/pkg/filetree/filetreeutil/meta"
	"a4.io/blobstash/pkg/filetree/writer"

	"context"
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
	bs  *blobstore.BlobStore
	kvs *kvstore.KvStore
}

func (*filetreeLsCmd) Name() string     { return "filetree-ls" }
func (*filetreeLsCmd) Synopsis() string { return "Display recent blobs" }
func (*filetreeLsCmd) Usage() string {
	return `recent :
	Display recent blobs.
`
}

func (*filetreeLsCmd) SetFlags(_ *flag.FlagSet) {}

func (r *filetreeLsCmd) Execute(_ context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	keys, err := r.kvs.Keys("_filetree:root:", "", "", -1)
	if err != nil {
		return rerr("failed to list keys: %v", err)
	}
	// TODO(tsileo): implements keysHandler
	for _, k := range keys {
		fmt.Printf("k=%+v\n", k)
	}
	// TODO(tsileo): support filetree-ls rootname/subdir
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
