package main

import (
  "os"
  "github.com/codegangsta/cli"
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
	    Name:      "add",
	    ShortName: "a",
	    Usage:     "add a task to the list",
	    Action: func(c *cli.Context) {
	    	info, err := os.Stat(c.Args().First())
	    	if os.IsNotExist(err) {
	    		println("No such file")
	    	}
	    	if err == nil {
	    		if info.IsDir() {
	    			println("it's a  dir")
		    	} else {
		    		println("it's a file")
		    	}
	    	}
	      println("added task: ", c.Args().First())
	    },
	  },
	  {
	    Name:      "complete",
	    ShortName: "c",
	    Usage:     "complete a task on the list",
	    Action: func(c *cli.Context) {
	      println("completed task: ", c.Args().First())
	    },
	  },
	}
 	app.Run(os.Args)
}