package luascripts

import (
	"testing"
)

const testLuaFile = `return function()
    return {{.expr}}
end
`

const testLuaFileExecuted = `return function()
    return true
end
`

func TestGet(t *testing.T) {
	dat := Get("test.lua")
	if dat != testLuaFile {
		t.Errorf("failed to get test.lua, %q", dat)
	}
}

func TestTpl(t *testing.T) {
	dat := Tpl("test.lua", Ctx{"expr": "true"})
	if dat != testLuaFileExecuted {
		t.Errorf("failed to get test.lua, %q", dat)
	}
}
