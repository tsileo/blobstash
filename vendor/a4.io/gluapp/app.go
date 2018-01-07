package gluapp

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/yuin/gopher-lua"
)

// App represents a Lua app
type App struct {
	ls            *lua.LState
	conf          *Config
	publicIndex   map[string]struct{}
	appEntrypoint string
}

func NewApp(conf *Config) (*App, error) {
	// Make some sanity checks
	if conf.Path == "" {
		return nil, fmt.Errorf("missing `conf.Path`")
	}
	epoint := "app.lua"
	if conf.Entrypoint != "" {
		epoint = conf.Entrypoint
	}
	appPath := filepath.Join(conf.Path, epoint)
	if _, err := os.Stat(appPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("app entrypoint not found (%s)", appPath)
	}

	// Initialize the app
	app := &App{
		conf:          conf,
		publicIndex:   map[string]struct{}{},
		appEntrypoint: appPath,
	}

	// If there's a public dir, fetch the list of files and keep them in an index
	publicPath, err := filepath.Abs(filepath.Join(conf.Path, "public"))
	if err != nil {
		return nil, err
	}
	_, err = os.Stat(publicPath)
	switch {
	case err == nil:
		if err := filepath.Walk(publicPath, func(path string, f os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !f.IsDir() {
				app.publicIndex[strings.Replace(path, publicPath, "", 1)] = struct{}{}
			}
			return nil
		}); err != nil {
			return nil, err
		}
	case os.IsNotExist(err):
	default:
		return nil, err
	}

	return app, nil
}

// Exec executes the app in the given context, but it does not write the output to the `http.ResponseWriter`,
// you need to call `Response.WriteTo(w)` manually.
//
// Most of the time, you should use `App` as a `http.HandlerFunc` (or call `App.ServeHTTP` manually).
func (a *App) Exec(w http.ResponseWriter, r *http.Request) (*Response, error) {
	path := r.URL.Path

	// First check if there the request match a file in public/
	if _, ok := a.publicIndex[path]; ok {
		http.ServeFile(w, r, filepath.Join(a.conf.Path, "public", path))
		return nil, nil
	}

	// Initialize a Lua state
	L := lua.NewState()
	defer L.Close()

	// Preload all the modules and setup global variables
	resp, err := setupState(L, a.conf, w, r)
	if err != nil {
		return nil, err
	}

	// Now we can execute the app entrypoint `app.lua`
	if err := L.DoFile(a.appEntrypoint); err != nil {
		// TODO(tsileo): display a nice stack trace in debug mode
		return nil, err
	}

	if a.conf.AfterScriptExecHook != nil {
		if err := a.conf.AfterScriptExecHook(L); err != nil {
			return nil, err
		}
	}

	return resp, nil
}

// ServeHTTP implements the `http.HandlerFunc` interface.
func (a *App) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	resp, err := a.Exec(w, r)
	if err != nil {
		panic(err)
	}

	// Write the request
	resp.WriteTo(w)
}
