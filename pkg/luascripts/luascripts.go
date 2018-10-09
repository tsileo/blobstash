package luascripts // import "a4.io/blobstash/pkg/luascripts"

import (
	"bytes"
	"fmt"
	"text/template"
)

func Get(name string) string {
	dat, ok := files[name]
	if !ok {
		panic(fmt.Sprintf("missing file %s", name))
	}
	return dat
}

type Ctx map[string]interface{}

func Tpl(name string, ctx Ctx) string {
	tpl := template.Must(template.New("").Parse(Get(name)))
	var buf bytes.Buffer
	if err := tpl.Execute(&buf, ctx); err != nil {
		panic(err)
	}
	return buf.String()
}
