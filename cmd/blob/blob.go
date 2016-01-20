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

	"github.com/tsileo/blobstash/client"

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
	// TODO(tsileo): host flag
	// and display an usage func
	// api key as a venv
	// and support an optional namespace as arg
	// Store the latest uploaded blob as feature? log the uploaded? with an optional comment (import vkv)
	// and write a README
	// sharing feature (with a blob app to register?)
	apiKey := os.Getenv("BLOB_API_KEY")
	flag.Usage = usage
	flag.Parse()
	if flag.NArg() < 1 {
		usage()
	}
	blobstore := client.NewBlobStore(os.Getenv("BLOB_API_HOST"))
	blobstore.SetAPIKey(apiKey)
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
