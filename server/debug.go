package server

import (
	"net/http"
	"fmt"
	"time"
	"log"
	"io"

	"github.com/bitly/go-notify"
)


func SendDebugData(data string) {
	cmd := fmt.Sprintf("%v: %v", time.Now().UTC().Format(time.RFC3339), data)
	notify.Post("monitor_cmd", cmd)
}

func handler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "")
}

func monitor(w http.ResponseWriter, r *http.Request) {
	log.Printf("server: starting HTTP monitoring %v", r.RemoteAddr)
	activeMonitorClient.Add(1)
	notifier := w.(http.CloseNotifier).CloseNotify()
	f, _ := w.(http.Flusher)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	newCmd := make(chan interface{})
	notify.Start("monitor_cmd", newCmd)
	var ls interface{}
	for {
		select {
		case ls = <-newCmd:
			io.WriteString(w, fmt.Sprintf("data: %v\n\n", ls.(string)))
			f.Flush()
		case <-notifier:
			log.Printf("server: HTTP monitoring %v disconnected", r.RemoteAddr)
			activeMonitorClient.Add(-1)
			break
		}
	}
}
