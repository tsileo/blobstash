/*

blob is a tiny command line too to interact with BlobStash blob store.

 - read from STDIN, upload and output hash
 - output raw blob from hash

 That's it!

*/
package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/tsileo/blobstash/client/blobstore"

	"github.com/dchest/blake2b"
)

func usage() {
	fmt.Fprintf(os.Stderr, "Usage: blob [options] [-|<hash>]\n")
	flag.PrintDefaults()
	os.Exit(2)
}

func main() {
	var ns string
	var comment string
	var save bool
	flag.StringVar(&ns, "ns", "", "Optional namespace (for upload)")
	flag.StringVar(&comment, "comment", "", "Optional comment (for upload)")
	flag.BoolVar(&save, "save", false, "Save the hash in the log (for upload)")
	// TODO(tsileo):
	// Store the latest uploaded blob as feature? log the uploaded? with an optional comment (import vkv)
	// sharing feature (with a blob app to register?)
	flag.Usage = usage
	flag.Parse()
	if flag.NArg() < 1 {
		usage()
	}
	opts := blobstore.DefaultOpts()
	opts.SetHost(os.Getenv("BLOB_API_HOST"), os.Getenv("BLOB_API_KEY"))
	opts.SetNamespace(ns)
	opts.SnappyCompression = false
	blobstore := blobstore.New(opts)
	if flag.Arg(0) == "-" {
		stdin, err := ioutil.ReadAll(os.Stdin)
		if err != nil {
			panic(err)
		}
		hash := fmt.Sprintf("%x", blake2b.Sum256(stdin))
		if err := blobstore.Put(hash, stdin); err != nil {
			panic(err)
		}
		fmt.Printf("%s", hash)
		os.Exit(0)
	}
	blob, err := blobstore.Get(flag.Arg(0))
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s", blob)
	os.Exit(0)
}
