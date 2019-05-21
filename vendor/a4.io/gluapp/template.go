package gluapp

import (
	"bytes"
	"html/template"
	"path/filepath"

	"a4.io/blobstash/pkg/apps/luautil"

	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/renderer/html"
	"github.com/yuin/gopher-lua"
)

var mdc = goldmark.New(
	goldmark.WithRendererOptions(
		html.WithHardWraps(),
		html.WithUnsafe(),
	),
)

var funcs = template.FuncMap{
	"markdownify": func(raw interface{}) template.HTML {
		var buf bytes.Buffer
		switch md := raw.(type) {
		case string:
			if err := mdc.Convert([]byte(md), &buf); err != nil {
				panic(err)
			}
			return template.HTML(buf.String())
		case lua.LString:
			if err := mdc.Convert([]byte(string(md)), &buf); err != nil {
				panic(err)
			}
			return template.HTML(buf.String())
		default:
			panic("bad md type")
		}
	},
	"htmlify": func(i string) template.HTML {
		return template.HTML(i)
	},
}

func setupTemplate(path string) func(*lua.LState) int {
	return func(L *lua.LState) int {
		// Setup the router module
		mod := L.SetFuncs(L.NewTable(), map[string]lua.LGFunction{
			"render_string": func(L *lua.LState) int {
				var out bytes.Buffer
				tpl, err := template.New("").Funcs(funcs).Parse(L.ToString(1))
				if err != nil {
					// TODO(tsileo): return error?
					return 0
				}
				if err := tpl.Execute(&out, luautil.TableToMap(L.ToTable(2))); err != nil {
					L.Push(lua.LString(err.Error()))
					return 1
				}
				L.Push(lua.LString(out.String()))
				return 1
			},
			"render": func(L *lua.LState) int {
				var out bytes.Buffer

				var templates []string
				for i := 1; i < L.GetTop(); i++ {
					templates = append(templates, filepath.Join(path, string(L.ToString(i))))
				}

				tmpl, err := template.New("").Funcs(funcs).ParseFiles(templates...)
				if err != nil {
					L.Push(lua.LString(err.Error()))
					return 1
				}
				tmplName := filepath.Base(templates[len(templates)-1])
				ctx := luautil.TableToMap(L.ToTable(L.GetTop()))
				if err := tmpl.ExecuteTemplate(&out, tmplName, ctx); err != nil {
					L.Push(lua.LString(err.Error()))
					return 1
				}

				L.Push(lua.LString(out.String()))
				return 1
			},
		})
		L.Push(mod)
		return 1
	}
}
