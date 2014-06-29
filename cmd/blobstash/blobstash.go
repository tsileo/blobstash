package main

import (
	"fmt"
	"os"
	"runtime"

	"github.com/codegangsta/cli"

	"github.com/tsileo/blobstash/client"
	"github.com/tsileo/blobstash/scheduler"
	"github.com/tsileo/blobstash/fs"
)

var nCPU = runtime.NumCPU()

func init() {
	if nCPU < 2 {
		nCPU = 2
	}
	runtime.GOMAXPROCS(nCPU)
}

func main() {
	ignoredFiles := []string{"*~", "*.py[cod]", "nohup.out", "*.log", "tmp_*"}
	app := cli.NewApp()
	commonFlags := []cli.Flag{
  		cli.StringFlag{"host", "", "override the real hostname"},
	}
	app.Name = "blobstash"
	app.Usage = "BlobStash command-line tool"
	app.Version = "0.1.0"
	//  app.Action = func(c *cli.Context) {
	//    println("Hello friend!")
	//  }
	app.Commands = []cli.Command{
		{
			Name:      "put",
			Usage:     "put a file/directory",
			Flags:     commonFlags,
			Action: func(c *cli.Context) {
				cl, _ := client.NewClient(c.String("host"), ignoredFiles)
				defer cl.Close()
				b, m, wr, err := cl.Put(&client.Ctx{Hostname: cl.Hostname}, c.Args().First())
				fmt.Printf("b:%+v,m:%+v,wr:%+v,err:%v\n", b, m, wr, err)
			},
		},
		//{
		//	Name:      "ls",
		//	Usage:     "List backups",
		//	Action: func(c *cli.Context) {
		//		client, _ := client.NewClient("", ignoredFiles)
		//		metas, _ := client.List()
		//		for _, m := range metas {
		//			fmt.Printf("%+v\n", m)
		//		}
		//	},
		//},
		{
			Name:      "restore",
			Usage:     "Restore a snapshot",
			Action: func(c *cli.Context) {
				cl, _ := client.NewClient("", ignoredFiles)
				defer cl.Close()
				args := c.Args()
				snap, meta, rr, err := cl.Get(args[0], args[1])
				fmt.Printf("snap:%+v,meta:%+v,rr:%+v/err:%v", snap, meta, rr, err)
			},
		},
		{
			Name:  "mount",
			Usage: "Mount the read-only filesystem to the given path",
			Action: func(c *cli.Context) {
				stop := make(chan bool, 1)
				stopped := make(chan bool, 1)
				fs.Mount(c.Args().First(), stop, stopped)
			},
		},
		{
			Name:      "scheduler",
			ShortName: "sched",
			Usage:     "Start the backup scheduler",
			Flags:     commonFlags,
			Action: func(c *cli.Context) {
				cl, _ := client.NewClient(c.String("host"), ignoredFiles)
				defer cl.Close()
				d := scheduler.New(cl)
				d.Run()
			},
		},
	}
	app.Run(os.Args)
}
