package template

import (
	"bytes"
	"html/template"
	"strings"

	"github.com/tsileo/blobstash/ext/lua/luautil"
	"github.com/yuin/gopher-lua"
)

var header = template.Must(template.New("header").Parse(`<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">

  <title>{{ .Title }}</title>

  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/pure/0.6.0/pure-min.css">
  {{ range .CSS }}
  <link rel="stylesheet" href="{{ . }}">
  {{ end }}
</head>
<body>
`))

var footer = template.Must(template.New("footer").Parse(`
</body>
</html>`))

type TplCtx struct {
	Title    string
	JS       []string
	CSS      []string
	JSBlocks []string
	Ctx      map[string]interface{}
}

type TemplateModule struct {
	ctx       *TplCtx
	Templates map[string]*template.Template
}

// TODO(tsileo) set purecss a default css
// See template.JS( before rendering

func New(templates map[string]*template.Template) *TemplateModule {
	return &TemplateModule{
		ctx:       &TplCtx{},
		Templates: templates,
	}
}

func (tpl *TemplateModule) Loader(L *lua.LState) int {
	mod := L.SetFuncs(L.NewTable(), map[string]lua.LGFunction{
		"render":   tpl.render,
		"settitle": tpl.settitle,
		// "addjs": tpl.addjs,
		"addcss": tpl.addcss,
		"setctx": tpl.setctx,
		// "addjsblock": tpl.addjsblock,
	})
	L.Push(mod)
	return 1
}

func (tpl *TemplateModule) setctx(L *lua.LState) int {
	tpl.ctx.Ctx = luautil.TableToMap(L.ToTable(1))
	return 0
}

func (tpl *TemplateModule) addcss(L *lua.LState) int {
	tpl.ctx.CSS = append(tpl.ctx.CSS, L.ToString(1))
	return 0
}

func (tpl *TemplateModule) settitle(L *lua.LState) int {
	tpl.ctx.Title = L.ToString(1)
	return 0
}

func (tpl *TemplateModule) render(L *lua.LState) int {
	var err error
	var ctpl *template.Template
	tplString := L.ToString(1)
	defaultTpl := true
	if strings.HasSuffix(tplString, ".tpl") {
		ctpl = tpl.Templates[tplString]
		defaultTpl = false
	} else {
		ctpl, err = template.New("tpl").Parse(tplString)
		if err != nil {
			panic(err)
		}
	}
	// TODO(tsileo) add some templatFuncs/template filter
	out := &bytes.Buffer{}
	if defaultTpl {
		if err := header.Execute(out, tpl.ctx); err != nil {
			panic(err)
		}
	}
	if err := ctpl.Execute(out, tpl.ctx.Ctx); err != nil {
		panic(err)
	}
	if defaultTpl {
		if err := footer.Execute(out, tpl.ctx); err != nil {
			panic(err)
		}
	}
	L.Push(lua.LString(out.String()))
	return 1
}
