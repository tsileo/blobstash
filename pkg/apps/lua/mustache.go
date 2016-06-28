package lua

import (
	"github.com/hoisie/mustache"
	"github.com/tsileo/blobstash/pkg/apps/luautil"
	"github.com/yuin/gopher-lua"
)

type MustacheModule struct {
	root string
}

func NewMustacheModule() *MustacheModule {
	return &MustacheModule{}
}

func (m *MustacheModule) Loader(L *lua.LState) int {
	mod := L.SetFuncs(L.NewTable(), map[string]lua.LGFunction{
		"render": m.render,
	})
	L.Push(mod)
	return 1
}

func (m *MustacheModule) render(L *lua.LState) int {
	L.Push(lua.LString(mustache.Render(L.ToString(1), luautil.TableToMap(L.ToTable(2)))))
	return 1
}
