// package util that regroups multiple useful Lua modules.
package util // import "a4.io/gluapp/util"

import (
	"crypto/rand"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/yuin/gopher-lua"
	"gopkg.in/yaml.v2"

	"a4.io/blobstash/pkg/apps/luautil"
)

func Setup(L *lua.LState, cwd string) {
	L.PreloadModule("cmd", setupCmd(cwd))
	L.PreloadModule("util", setupUtil(cwd))
}

// Return a module with a single "run" function that run CLI commands and return the error
// as a string.
func setupUtil(cwd string) func(*lua.LState) int {
	return func(L *lua.LState) int {
		// register functions to the table
		mod := L.SetFuncs(L.NewTable(), map[string]lua.LGFunction{
			"random_string": func(L *lua.LState) int {
				b := make([]byte, L.ToInt(1))
				if _, err := rand.Read(b); err != nil {
					panic(err)
				}
				L.Push(lua.LString(fmt.Sprintf("%x", b)))
				return 1
			},
			"read_yaml": func(L *lua.LState) int {
				data, err := ioutil.ReadFile(filepath.Join(cwd, L.ToString(1)))
				if err != nil {
					panic(err)
				}
				res := map[string]interface{}{}
				if err := yaml.Unmarshal([]byte(data), &res); err != nil {
					panic(err)
				}
				L.Push(luautil.InterfaceToLValue(L, res))
				return 1
			},
			"delete_file": func(L *lua.LState) int {
				if err := os.Remove(filepath.Join(cwd, L.ToString(1))); err != nil {
					panic(err)
				}
				return 0
			},
			"read_file": func(L *lua.LState) int {
				data, err := ioutil.ReadFile(filepath.Join(cwd, L.ToString(1)))
				if err != nil {
					panic(err)
				}
				L.Push(lua.LString(data))
				return 1
			}})
		// returns the module
		L.Push(mod)
		return 1
	}
}

// Return a module with a single "run" function that run CLI commands and return the error
// as a string.
func setupCmd(cwd string) func(*lua.LState) int {
	return func(L *lua.LState) int {
		// register functions to the table
		mod := L.SetFuncs(L.NewTable(), map[string]lua.LGFunction{
			"run": func(L *lua.LState) int {
				parts := strings.Split(L.ToString(1), " ")
				cmd := exec.Command(parts[0], parts[1:]...)
				cmd.Dir = cwd
				err := cmd.Run()
				var out string
				if err != nil {
					out = err.Error()
				}
				L.Push(lua.LString(out))
				return 1
			},
		})
		// returns the module
		L.Push(mod)
		return 1
	}
}
