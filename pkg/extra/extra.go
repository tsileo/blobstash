package extra // import "a4.io/blobstash/pkg/extra"

import (
	"crypto/rand"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/yuin/gopher-lua"
	"willnorris.com/go/microformats"
)

type Extra struct{}

func setupExtra(e *Extra) func(*lua.LState) int {
	return func(L *lua.LState) int {
		// register functions to the table
		mod := L.SetFuncs(L.NewTable(), map[string]lua.LGFunction{
			"noop": func(L *lua.LState) int {
				return 0
			},
			"glob": func(L *lua.LState) int {
				// match(<glob pattern>, <name>)
				matched, err := filepath.Match(L.ToString(1), L.ToString(2))
				if err != nil {
					panic(err)
				}
				if matched {
					L.Push(lua.LTrue)
				} else {
					L.Push(lua.LFalse)
				}
				return 1
			},
			"format_datetime": func(L *lua.LState) int {
				dt := L.ToString(1)
				layout := L.ToString(2)
				t, err := time.Parse(layout, dt)
				if err != nil {
					panic(err)
				}

				L.Push(lua.LString(t.Format(L.ToString(3))))

				return 1
			},
			"split": func(L *lua.LState) int {
				tbl := L.NewTable()
				for _, part := range strings.Split(L.ToString(1), L.ToString(2)) {
					tbl.Append(lua.LString(part))
				}
				L.Push(tbl)
				return 1
			},
			"now": func(L *lua.LState) int {
				L.Push(lua.LNumber(time.Now().Unix()))
				return 1
			},
			"random": func(L *lua.LState) int {
				raw := make([]byte, L.ToInt(1))
				if _, err := rand.Read(raw); err != nil {
					panic(err)
				}
				out := fmt.Sprintf("%x", raw)
				L.Push(lua.LString(out))
				return 1
			},
			"v": func(L *lua.LState) int {
				L.Push(lua.LString(fmt.Sprintf("%v", time.Now().UnixNano())))
				return 1
			},
			"parse_microformats": func(L *lua.LState) int {
				req, err := http.NewRequest("GET", L.ToString(1), nil)
				if err != nil {
					panic(err)
				}
				// TODO(tsileo): set a user agent
				// req.Header.Set("User-Agent", UserAgent)

				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					panic(err)
				}
				defer resp.Body.Close()

				data := microformats.Parse(resp.Body, resp.Request.URL)

				rels := L.NewTable()
				for rel, links := range data.Rels {
					ls := L.NewTable()
					for _, link := range links {
						ls.Append(lua.LString(link))
					}
					rels.RawSetString(rel, ls)
				}

				tbl := L.NewTable()
				tbl.RawSetString("rels", rels)
				L.Push(tbl)

				return 1
			},
		})
		// returns the module
		L.Push(mod)
		return 1
	}
}

func Setup(L *lua.LState) *Extra {
	e := &Extra{}
	// luautil.InterfaceToLValue(L, nil)
	L.PreloadModule("extra", setupExtra(e))
	return e
}
