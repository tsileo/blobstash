package main

import (
	"fmt"
	"github.com/codegangsta/cli"
	"github.com/tsileo/datadatabase/client"
	"github.com/tsileo/datadatabase/daemon"
	"github.com/tsileo/datadatabase/fs"
	"os"
)

func main() {
	ignoredFiles := []string{"*~", "*.py[cod]", "nohup.out", "*.log", "tmp_*"}
	app := cli.NewApp()
	app.Name = "datadb"
	app.Usage = "DataDB client"
	app.Version = "0.1.0"
	//  app.Action = func(c *cli.Context) {
	//    println("Hello friend!")
	//  }
	app.Commands = []cli.Command{
		{
			Name:      "put",
			ShortName: "put",
			Usage:     "put a file/directory",
			Action: func(c *cli.Context) {
				client, _ := client.NewClient(ignoredFiles)
				b, m, wr, err := client.Put(c.Args().First())
				fmt.Printf("b:%+v,m:%+v,wr:%+v,err:%v\n", b, m, wr, err)
			},
		},
		{
			Name:      "ls",
			ShortName: "ls",
			Usage:     "List backups",
			Action: func(c *cli.Context) {
				client, _ := client.NewClient(ignoredFiles)
				//metas, _ := client.List()
				//for _, m := range metas {
				//	fmt.Printf("%+v\n", m)
				//}
				rr, err := client.GetDir(c.Args().First(), "/tmp/testtonalityrestoreomg")
				fmt.Printf("rr:%+v/err:%v", rr, err)
			},
		},
		{
			Name:  "mount",
			Usage: "Mount the read-only filesystem to the given path",
			Action: func(c *cli.Context) {
				fs.Mount(c.Args().First())
			},
		},
		{
			Name:      "daemon",
			ShortName: "daemon",
			Usage:     "Snapshot daemon",
			Action: func(c *cli.Context) {
				client, _ := client.NewClient(ignoredFiles)
				d := daemon.New(client)
				d.Run()
			},
		},
		{
			Name:      "test",
			ShortName: "test",
			Usage:     "test",
			Action: func(c *cli.Context) {
				h, _ := os.Hostname()
				fmt.Printf("hostname: %v", h)
			},
		},
	}
	app.Run(os.Args)
}
