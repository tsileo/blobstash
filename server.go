package main

import (
	"github.com/tsileo/datadatabase/server"
)

func main() {
	stop := make(chan bool)
	server.New("127.0.0.1:9736", stop)
}
