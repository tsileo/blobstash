package main

import (
	"fmt"
	"os"

	"a4.io/blobstash/pkg/client/blobstore"
	"a4.io/blobstash/pkg/client/clientutil"
	"a4.io/blobstash/pkg/client/filetree"
	_ "a4.io/blobstash/pkg/client/kvstore"
	rnode "a4.io/blobstash/pkg/filetree/filetreeutil/node"
	"a4.io/blobstash/pkg/filetree/writer"
)

func main() {
	// TODO(tsileo) config file with server address and collection name
	host := os.Getenv("BLOBS_API_HOST")
	apiKey := os.Getenv("BLOBS_API_KEY")

	c := clientutil.NewClientUtil(host, clientutil.WithAPIKey(apiKey), clientutil.WithNamespace("myns2"))
	bs := blobstore.New(c)
	ft := filetree.New(c)
	fmt.Printf("ft=%+v\n", ft)
	// kvs := kvstore.New(c)
	finfo, err := os.Stat(os.Args[1])
	switch {
	case os.IsNotExist(err):
		fmt.Printf("path \"%s\" does not exist", os.Args[1])
		return
	case err == nil:
	default:
		fmt.Printf("failed to stat file: %v", err)
		return
	}
	var m *rnode.RawNode
	up := writer.NewUploader(bs)
	// It's a dir
	if finfo.IsDir() {
		m, err = up.PutDir(os.Args[1])
		if err != nil {
			fmt.Printf("failed to upload: %v", err)
			return
		}
	} else {
		// It's a file
		m, err = up.PutFile(os.Args[1])
		if err != nil {
			fmt.Printf("failed to upload: %v", err)
			return
		}
	}
	rev, err := ft.MakeSnapshot(m.Hash, "backup1", "lol msg")
	if err != nil {
		panic(err)
	}
	if err := ft.GC("myns2", "backup1", rev); err != nil {
		panic(err)
	}

	fmt.Printf("meta=%+v\nrev=%v", m, rev)
}
