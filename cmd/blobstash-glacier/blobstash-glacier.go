package main

import (
	"fmt"
	"os"

	"github.com/codegangsta/cli"

	"github.com/tsileo/blobstash/backend/glacier/util"
)

func main() {
	app := cli.NewApp()
	app.Name = "blobstash-glacier"
	app.Usage = "BlobStash glacier restore tools"
	app.Version = "0.1.0"
	app.Commands = []cli.Command{
		{
			Name:      "status",
			ShortName: "status",
			Usage:     "display information about the given vault",
			Action: func(c *cli.Context) {
				con := util.GetCon("")
				vault, _ := con.DescribeVault(c.Args().First())
				fmt.Printf("Vault name:         : %v\n", vault.VaultName)
				fmt.Printf("Creation date:      : %v\n", vault.CreationDate)
				fmt.Printf("Last inventory date : %v\n", vault.LastInventoryDate)
				fmt.Printf("Number of archives  : %v\n", vault.NumberOfArchives)
				fmt.Printf("Size                : %v\n", vault.SizeInBytes)
			},
		},
		{
			Name:      "sync",
			ShortName: "sync",
			Usage:     "fetch the latest inventory and sync the local database",
			Action: func(c *cli.Context) {
				con := util.GetCon("")
				db, err := util.GetDB()
				defer db.Close()
				if err != nil {
					panic(err)
				}
				if err := util.Sync(con, db, c.Args().First()); err != nil {
					panic(err)
				}
			},
		},
		{
			Name:      "restore",
			ShortName: "restore",
			Usage:     "restore previously synced archives from local database",
			Action: func(c *cli.Context) {
				con := util.GetCon("")
				db, err := util.GetDB()
				defer db.Close()
				if err != nil {
					panic(err)
				}
				if err := util.Restore(con, db, c.Args().First()); err != nil {
					panic(err)
				}
			},
		},
	}
	app.Run(os.Args)
}
