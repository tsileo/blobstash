package main

import (
	"os"
	"github.com/codegangsta/cli"
	"github.com/tsileo/datadatabase/models"
	"fmt"
)

func main() {
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
	    	client, _ := models.NewClient()
	    	b, m, wr, err := client.Put(c.Args().First())
	    	fmt.Printf("b:%+v,m:%+v,wr:%+v,err:%v\n", b, m, wr, err)
	    },
	  },
	  {
	    Name:      "ls",
	    ShortName: "ls",
	    Usage:     "List backups",
	    Action: func(c *cli.Context) {
	    	client, _ := models.NewClient()
	    	metas, _ := client.List()
	    	for _, m := range metas {
	    		fmt.Printf("%+v\n", m)
	    	}
	    },
	  },
	}
 	app.Run(os.Args)
}