package main

import (
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/codegangsta/cli"
	"github.com/tsileo/blobstash/backend/blobsfile"
)

var version = "dev"

func main() {
	app := cli.NewApp()
	commonFlags := []cli.Flag{
	//		cli.StringFlag{"host", "", "override the real hostname"},
	//		cli.StringFlag{"config", "", "config file"},
	}
	app.Name = "blobsfile"
	app.Usage = "blobsfile format debug command-line tool"
	app.Version = version
	app.Commands = []cli.Command{
		{
			Name:  "ls",
			Usage: "List all hashes",
			Flags: commonFlags,
			Action: func(c *cli.Context) {
				path := c.Args().First()
				indexFile := filepath.Join(path, "blobs-index")
				if _, err := os.Stat(indexFile); os.IsNotExist(err) {
					fmt.Printf("Index file not found at %v, aborting", indexFile)
					return
				}
				index, err := blobsfile.NewIndex(path)
				defer index.Close()
				if err != nil {
					fmt.Printf("Failed to load index: %v, aborting", err)
					return
				}
				enum, _, err := index.DB().Seek(index.FormatBlobPosKey(""))
				if err != nil {
					return
				}
				for {
					k, _, err := enum.Next()
					if err == io.EOF {
						break
					}
					// Remove the BlobPosKey prefix byte
					fmt.Printf("%s\n", hex.EncodeToString(k[1:]))
				}
			},
		},
		{
			Name:  "get",
			Usage: "Get blob content",
			Flags: commonFlags,
			Action: func(c *cli.Context) {
				path := c.Args().First()
				hash := c.Args().Get(1)
				conf := &blobsfile.Config{Dir: path}
				backend := blobsfile.NewFromConfig(conf.Map())
				blob, err := backend.Get(hash)
				defer backend.Close()
				if err != nil {
					fmt.Printf("failed to get blob %v: %v", hash, err)
					return
				}
				fmt.Printf("%s", blob)
			},
		},
	}
	app.Run(os.Args)
}
