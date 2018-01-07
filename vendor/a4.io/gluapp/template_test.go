package gluapp

import (
	"testing"

	"github.com/yuin/gopher-lua"
)

func TestTemplate(t *testing.T) {
	// Create a new empty state
	L := lua.NewState()
	defer L.Close()

	// Setup the state
	L.PreloadModule("template", setupTemplate("tests_data/"))
	setupTestState(L, t)

	// Execute the Lua code
	if err := L.DoString(`
tpl = require('template')

logf('testing template...')

out = tpl.render_string('Hello {{.world}} from template', {world = 'World'})
expected = 'Hello World from template'
if out ~= expected then
  errorf('template.render_string error, got %v, expected %v', out, expected)
end

out = tpl.render('hello.html', {world = 'World'})
expected = 'Hello World!\n'
if out ~= expected then
  errorf('template.render (1) error, got %v, expected %v', out, expected)
end

out2 = tpl.render('page1.html', 'layout.html', {world = 'World'})
if out2 ~= expected then
  errorf('template.render (2) error, got %v, expected %v', out2, expected)
end

-- TODO(tsileo): also test error behavior
`); err != nil {
		panic(err)
	}
}
