package main

import (
	"flag"
	"fmt"
	"os"

	"a4.io/blobstash/pkg/client/blobstore"
	"a4.io/blobstash/pkg/client/clientutil"
	"a4.io/blobstash/pkg/client/filetree"
	rnode "a4.io/blobstash/pkg/filetree/filetreeutil/node"
	"a4.io/blobstash/pkg/filetree/writer"
)

const ua = "blobstash-uploader v1"

func usage() {
	fmt.Printf("Usage: %s [OPTIONS] [FSNAME] [DIRPATH]\n", os.Args[0])
	flag.PrintDefaults()
}

var snapMessage string

func main() {
	flag.Usage = usage
	flag.StringVar(&snapMessage, "message", "", "Optional snapshot message")
	flag.Parse()

	if flag.NArg() != 2 {
		usage()
		os.Exit(2)
	}

	host := os.Getenv("BLOBSTASH_API_HOST")
	apiKey := os.Getenv("BLOBSTASH_API_KEY")
	fsName := flag.Arg(0)
	dirPath := flag.Arg(1)

	if host == "" {
		fmt.Printf("no server configure, please set BLOBSTASH_API_{HOST|KEY}\n")
		os.Exit(1)
	}

	c := clientutil.NewClientUtil(host,
		clientutil.WithAPIKey(apiKey),
		clientutil.WithNamespace(fsName))

	authOk, err := c.CheckAuth()
	if err != nil {
		fmt.Printf("failed to check authentication: %v\n", err)
		os.Exit(1)
	}

	if !authOk {
		fmt.Printf("bad API key")
		os.Exit(1)
	}

	bs := blobstore.New(c)
	ft := filetree.New(c)

	// Src sanity check
	finfo, err := os.Stat(dirPath)
	switch {
	case os.IsNotExist(err):
		fmt.Printf("path \"%s\" does not exist\n", dirPath)
		os.Exit(1)
	case err == nil:
	default:
		fmt.Printf("failed to stat file: %v\n", err)
		os.Exit(1)
	}
	if !finfo.IsDir() {
		fmt.Printf("can only backup directories\n")
		os.Exit(1)
	}

	var m *rnode.RawNode
	up := writer.NewUploader(bs)

	// Upload the tree
	m, err = up.PutDir(dirPath)
	if err != nil {
		fmt.Printf("failed to upload: %v\n", err)
		os.Exit(1)
	}

	// Make a snaphot/create a FS entry for the given tree
	rev, err := ft.MakeSnapshot(m.Hash, fsName, snapMessage, ua)
	if err != nil {
		fmt.Printf("failed to create snapshot: %v\n", err)
		os.Exit(1)
	}

	// The GC step will actually save the tree, as we're working within a namespace
	if err := ft.GC(fsName, fsName, rev); err != nil {
		fmt.Printf("failed to perform GC: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Backup successful,\nroot=%s\nrev=%d\n", m.Hash, rev)
	os.Exit(0)
}
