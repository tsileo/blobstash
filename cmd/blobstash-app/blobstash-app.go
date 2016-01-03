package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"os"
	"os/signal"

	"gopkg.in/fsnotify.v1"
)

// FIXME(tsileo) watch seems to be broken
// TODO(tsileo) better error handling

type Lvl int

const (
	LvlCrit = iota // May be used for logging panic?
	LvlError
	LvlWarn
	LvlInfo
	LvlDebug
)

// Returns the name of a Lvl
func (l Lvl) String() string {
	switch l {
	case LvlDebug:
		return "dbug"
	case LvlInfo:
		return "info"
	case LvlWarn:
		return "warn"
	case LvlError:
		return "eror"
	case LvlCrit:
		return "crit"
	default:
		panic("bad level")
	}
}

var Usage = func() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "SUBCOMMAND:\n\tapps stats register watch\n")
	flag.PrintDefaults()
}

var client = &http.Client{}

// FIXME(tsileo) store these in a JSON file
var server = os.Getenv("BLOBSTASH_APP_SERVER")
var defaultServer = "http://localhost:8050"
var apiKey = os.Getenv("BLOBSTASH_APP_API_KEY")

func watch(appID, path string, public, inMem bool) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		panic(err)
	}
	defer watcher.Close()

	done := make(chan bool)
	go func() {
		for {
			select {
			case event := <-watcher.Events:
				// fmt.Printf("event:%v\n", event)
				if event.Op&fsnotify.Write == fsnotify.Write || event.Op&fsnotify.Rename == fsnotify.Rename {
					// fmt.Printf("modified file:%v\n", event.Name)
					register(appID, path, public, inMem)
					log.Println("App updated")
				}
			case err := <-watcher.Errors:
				fmt.Printf("err:%v\n", err)
			case <-done:
				fmt.Println("Exiting...")
			}
		}
	}()

	err = watcher.Add(path)
	if err != nil {
		panic(err)
	}
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	block := make(chan bool)
	go func() {
		for _ = range c {
			done <- true
			block <- true
			// sig is a ^C, handle it
		}
	}()
	<-block
}

func register(appID, path string, public, inMem bool) {
	var b bytes.Buffer
	w := multipart.NewWriter(&b)
	// Add your image file
	f, err := os.Open(path)
	if err != nil {
		return
	}
	fw, err := w.CreateFormFile("script", path)
	if err != nil {
		return
	}
	if _, err = io.Copy(fw, f); err != nil {
		return
	}
	f.Close()
	w.Close()
	request, err := http.NewRequest("POST", server+fmt.Sprintf("/api/ext/lua/v1/register?appID=%s&public=%v&in_memory=%v", appID, public, inMem), &b)
	request.Header.Set("Content-Type", w.FormDataContentType())
	request.SetBasicAuth("", apiKey)
	if err != nil {
		panic(err)
	}
	resp, err := client.Do(request)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode == 200:
		fmt.Printf("App %v registered\n", appID)
	default:
		fmt.Printf("failed:%+v", resp)
		panic("req failed")
	}
}

func logs(appID string) {
	request, err := http.NewRequest("GET", server+"/api/ext/lua/v1/logs?appID="+appID, nil)
	request.SetBasicAuth("", apiKey)
	if err != nil {
		panic(err)
	}
	resp, err := client.Do(request)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode == 200:
		res := map[string]interface{}{}
		if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
			panic(err)
		}
		for _, ilog := range res["logs"].([]interface{}) {
			log := ilog.(map[string]interface{})
			fmt.Printf("%v %v %v %v\n", log["time"].(string), Lvl(log["lvl"].(float64)).String(), log["req_id"].(string), log["message"].(string))
		}
	case resp.StatusCode == 404:
		panic("404")
	default:
		fmt.Printf("failed:%+v", resp)
	}
}

func stats(appID string) {
	request, err := http.NewRequest("GET", server+"/api/ext/lua/v1/stats?appID="+appID, nil)
	request.SetBasicAuth("", apiKey)
	if err != nil {
		panic(err)
	}
	resp, err := client.Do(request)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode == 200:
		res := map[string]interface{}{}
		if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
			panic(err)
		}
		for k, v := range res {
			fmt.Printf("%v: %v\n", k, v)
		}
	case resp.StatusCode == 404:
		panic("404")
	default:
		fmt.Printf("failed:%+v", resp)
	}
}

func apps() {
	request, err := http.NewRequest("GET", server+"/api/ext/lua/v1/", nil)
	request.SetBasicAuth("", apiKey)
	if err != nil {
		panic(err)
	}
	resp, err := client.Do(request)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode == 200:
		res := map[string]interface{}{}
		if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
			panic(err)
		}
		// fmt.Printf("res:%+v\n\n\n", res)
		for _, iapp := range res["apps"].([]interface{}) {
			app := iapp.(map[string]interface{})
			fmt.Printf("%v [public=%v,request=%d]\n", app["app_id"].(string), app["is_public"].(bool), int(app["stats_requests"].(float64)))
		}
	case resp.StatusCode == 404:
		panic("404")
	default:
		fmt.Printf("failed:%+v", resp)
	}
}

func main() {
	if server == "" {
		server = defaultServer
	}
	if apiKey == "" {
		panic("missing API key")
	}
	publicPtr := flag.Bool("public", false, "Make the app public (watch/register)")
	inMemPtr := flag.Bool("in-mem", false, "Don't store the app (register)")
	// FIXME(tsileo) a config subcommand that generate a JSON file
	flag.Usage = Usage
	flag.Parse()

	// fmt.Printf("DEBUG public=%v&in_mem=%v\n", *publicPtr, *inMemPtr)

	if flag.NArg() < 1 {
		Usage()
		os.Exit(2)
	}

	switch flag.Arg(0) {
	case "apps":
		apps()
	case "stats":
		if flag.NArg() != 2 {
			fmt.Println("stats [AppID]")
			return
		}
		stats(flag.Arg(1))
	case "logs":
		if flag.NArg() != 2 {
			fmt.Println("logs [AppID]")
			return
		}
		logs(flag.Arg(1))
	case "register":
		switch flag.NArg() {
		case 1:
			fmt.Println("register [AppID] [/path/to/file.lua]")
		case 3:
			appID := flag.Arg(1)
			path := flag.Arg(2)
			register(appID, path, *publicPtr, *inMemPtr)
		default:
		}
	case "watch":
		switch flag.NArg() {
		case 1:
			fmt.Println("register [AppID] [/path/to/file.lua]")
		case 3:
			appID := flag.Arg(1)
			path := flag.Arg(2)
			register(appID, path, *publicPtr, true)
			fmt.Printf("watching for update...\n")
			watch(appID, path, *publicPtr, true)
		default:
		}
	case "remove":
		// FIXME(tsileo) implement this
		fmt.Println("TO BE DONE")
	default:
		fmt.Println("No such subcommand")
	}
}
