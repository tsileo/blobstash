package app

import (
	"crypto/md5"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/cjoudrey/gluahttp"
	log "github.com/inconshreveable/log15"
	"github.com/yuin/gopher-lua"
	"golang.org/x/net/context"

	luamod "github.com/tsileo/blobstash/pkg/apps/lua"
	"github.com/tsileo/blobstash/pkg/apps/luautil"
	"github.com/tsileo/blobstash/pkg/httputil"
)

func handle404(w http.ResponseWriter) {
	w.WriteHeader(http.StatusNotFound)
	w.Write([]byte(http.StatusText(http.StatusNotFound)))
}

type EntryPoint struct {
	Index    string `yaml:"index"`
	CatchAll bool   `yaml:"catch_all"`
	Domain   string `yaml:"domain"`
}

// App handle an app meta data
type App struct {
	Name       string
	EntryPoint *EntryPoint
	Config     map[string]interface{}
	auth       func(*http.Request) bool

	pathFunc func(string) (AppNode, error)

	log log.Logger
	mu  sync.Mutex
}

func New(name string, entrypoint *EntryPoint, config map[string]interface{}, pathFunc func(string) (AppNode, error), authFunc func(*http.Request) bool) *App {
	return &App{
		Name:       name,
		EntryPoint: entrypoint,
		Config:     config,
		pathFunc:   pathFunc,
		auth:       authFunc,
		mu:         sync.Mutex{},
	}
}

type ReadSeekCloser interface {
	io.Reader
	io.Seeker
	io.Closer
}
type AppNode interface {
	Reader() ReadSeekCloser
	IsDir() bool
	Name() string
	ModTime() time.Time
}

func (app *App) Serve(ctx context.Context, w http.ResponseWriter, req *http.Request) {
	p := req.URL.Path
	if app.auth != nil {
		if !app.auth(req) {
			w.Header().Set("WWW-Authenticate", fmt.Sprintf("Basic realm=\"BlobStash App %s\"", app.Name))
			w.WriteHeader(http.StatusUnauthorized)
		}
		return
	}

	// Clean the path and check there's no double dot
	p = path.Clean(p)
	if containsDotDot(p) {
		w.WriteHeader(500)
		w.Write([]byte("Invalid URL path"))
	}

	if app.EntryPoint != nil && app.EntryPoint.CatchAll {
		f, err := app.pathFunc(app.EntryPoint.Index)
		if err != nil {
			panic(err)
		}
		reader := f.Reader()
		defer reader.Close()
		script, err := ioutil.ReadAll(reader)
		if err != nil {
			panic(err)
		}
		app.doLua(string(script), req, w)
		return
	}

	// Determine the default entry point if the path is the root
	if p == "/" {
		p = "/index.html"
		if app.EntryPoint != nil && app.EntryPoint.Index != "" {
			// if !strings.HasPrefix(app.entrypoint, "/") {
			// 	w.WriteHeader(500)
			// 	w.Write([]byte("Invalid app entrypoint, must start with a /"))
			// 	return
			// }
			p = app.EntryPoint.Index
		}
	}

	// Inspect the file
	// app.log.Info("serve", "path", p)
	f, err := app.pathFunc(p)
	if err != nil {
		panic(err)
	}
	if f == nil {
		handle404(w)
	}
	// if fi, ok := app.index[p[1:]]; ok {
	// 	// Open the file
	// 	f, err := os.Open(filepath.Join(app.path, p))
	// 	defer f.Close()
	// 	if err != nil {
	// 		panic(err)
	// 	}
	reader := f.Reader()
	// The node is a Lua script, execute it
	if strings.HasSuffix(p, ".lua") {
		// TODO(tsileo): handle caching
		script, err := ioutil.ReadAll(reader)
		if err != nil {
			panic(err)
		}
		app.doLua(string(script), req, w)
		return
	}

	// The node is a dir, display the file content in a really basic HTML page
	if f.IsDir() {
		// TODO(tsileo): logs a warning
		return
	}

	// The node is a file
	httputil.SetAttachment(f.Name(), req, w)
	// TODO(tsileo): support resizing
	http.ServeContent(w, req, f.Name(), f.ModTime(), reader)
	return
}

// Execute the Lua script contained in the script
func (app *App) doLua(script string, r *http.Request, w http.ResponseWriter) error {
	// start := time.Now()
	L := lua.NewState()
	defer L.Close()

	L.SetGlobal("unix", L.NewFunction(func(L *lua.LState) int {
		L.Push(lua.LNumber(time.Now().Unix()))
		return 1
	}))

	L.SetGlobal("print", L.NewFunction(func(L *lua.LState) int {
		// TODO(tsileo): get the number of items in L, put them in a slice, and call fmt.Sprintf
		// app.log.Info(L.ToString(1))
		return 0
	}))
	L.SetGlobal("md5", L.NewFunction(func(L *lua.LState) int {
		L.Push(lua.LString(fmt.Sprintf("%x", md5.Sum([]byte(L.ToString(1))))))
		return 1
	}))
	// Make the config map defined in the config available from the script
	L.SetGlobal("config", luautil.InterfaceToLValue(L, app.Config))

	// XXX(tsileo): css preprocessing? gocss/less/saas?

	// TODO(tsileo): handle basic auth/api key via config and implement it as a middleware
	// TODO(tsileo): blobstore, kvstore, docstore, filetree module
	// TODO(tsileo): bewit module
	// TODO(tsileo): build a tiny kv wrapper for temp data, e.g. link shortener
	// TODO(tsileo): build a tiny "json" module using luautil
	L.PreloadModule("http", gluahttp.NewHttpModule(&http.Client{}).Loader)

	// Load the blobstash module
	response := luamod.NewResponseModule()
	L.PreloadModule("response", response.Loader)

	request := luamod.NewRequestModule(r)
	L.PreloadModule("request", request.Loader)

	// fs := luamod.NewFSModule(app.path)
	// L.PreloadModule("fs", fs.Loader)

	mustache := luamod.NewMustacheModule()
	L.PreloadModule("mustache", mustache.Loader)

	// Execute the script
	if err := L.DoString(script); err != nil {
		// TODO(tsileo): enable caching with TTL
		// FIXME(tsileo): better error, with debug mode?
		panic(err)
	}
	response.WriteTo(w)
	// app.log.Info("script executed", "time", time.Since(start), "script", script)
	return nil
}

// borrowed from net/http
func containsDotDot(v string) bool {
	if !strings.Contains(v, "..") {
		return false
	}
	for _, ent := range strings.FieldsFunc(v, isSlashRune) {
		if ent == ".." {
			return true
		}
	}
	return false
}

func isSlashRune(r rune) bool { return r == '/' || r == '\\' }
