package gluapp

import (
	"bytes"
	"fmt"
	"html/template"
	"path/filepath"
	"strings"
	"time"

	"a4.io/blobstash/pkg/apps/luautil"
	"mvdan.cc/xurls"

	"github.com/yuin/goldmark"
	highlighting "github.com/yuin/goldmark-highlighting"
	"github.com/yuin/goldmark/extension"
	"github.com/yuin/goldmark/renderer/html"
	"github.com/yuin/gopher-lua"
)

var mdc = goldmark.New(
	goldmark.WithRendererOptions(
		html.WithHardWraps(),
		html.WithUnsafe(),
	),
	goldmark.WithExtensions(
		highlighting.Highlighting,
		extension.Table,
		extension.NewLinkify(
			extension.WithLinkifyAllowedProtocols([][]byte{
				[]byte("http:"),
				[]byte("https:"),
			}),
			extension.WithLinkifyURLRegexp(
				xurls.Strict,
			),
		),
	),
)

var funcs = template.FuncMap{
	"markdownify": func(raw interface{}) template.HTML {
		var buf bytes.Buffer
		md := raw.(string)
		if err := mdc.Convert([]byte(strings.Replace(md, "\r\n", "\n", -1)), &buf); err != nil {
			panic(err)
		}
		return template.HTML(buf.String())
	},
	"htmlify": func(i string) template.HTML {
		return template.HTML(i)
	},
	"format_ts": func(ts float64, fmt string) string {
		return time.Unix(int64(ts), 0).Format(fmt)
	},
	"format": func(v interface{}, f string) string {
		return fmt.Sprintf(f, v)
	},
}

func setupTemplate(path string, funcMap template.FuncMap) func(*lua.LState) int {
	return func(L *lua.LState) int {
		// Setup the router module
		mod := L.SetFuncs(L.NewTable(), map[string]lua.LGFunction{
			"render_string": func(L *lua.LState) int {
				var out bytes.Buffer
				tpl, err := template.New("").Funcs(funcMap).Parse(L.ToString(1))
				if err != nil {
					// TODO(tsileo): return error?
					return 0
				}
				if err := tpl.Execute(&out, luautil.TableToMap(L, L.ToTable(2))); err != nil {
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
					// FIXME: remove dot in the filename
					templates = append(templates, filepath.Join(path, string(L.ToString(i))))
				}

				tmpl, err := template.New("").Funcs(funcMap).ParseFiles(templates...)
				if err != nil {
					L.Push(lua.LString(err.Error()))
					return 1
				}
				tmplName := filepath.Base(templates[len(templates)-1])
				ctx := luautil.TableToMap(L, L.ToTable(L.GetTop()))
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
