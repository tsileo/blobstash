/*

blob is a tiny command line too to interact with BlobStash blob store.

 - read from STDIN, upload and output hash
 - output raw blob from hash

 That's it!

*/
package main

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/tsileo/blobstash/client"

	"github.com/dchest/blake2b"
)

func main() {
	// TODO(tsileo): host flag
	// and display an usage func
	// api key as a venv
	// and support an optional namespace as arg
	// Store the latest uploaded blob as feature? log the uploaded? with an optional comment (import vkv)
	// and write a README
	// sharing feature (with a blob app to register?)
	apiKey := os.Getenv("BLOB_API_KEY")
	if len(os.Args) < 2 {
		fmt.Println("Missing hash, use \"-\" to upload from stdin")
		os.Exit(1)
	}
	blobstore := client.NewBlobStore("")
	blobstore.SetAPIKey(apiKey)
	if os.Args[1] == "-" {
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
	blob, err := blobstore.Get(os.Args[1])
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s", blob)
	os.Exit(0)
}
