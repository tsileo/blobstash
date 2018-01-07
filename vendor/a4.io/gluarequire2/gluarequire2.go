package gluarequire2 // import "a4.io/gluarequire2"

import (
	"fmt"

	"github.com/yuin/gopher-lua"
)

type RequireSetuper interface {
	Setup(L *lua.LState, name string) (newName string, err error)
}

type require2 struct {
	RequireSetuper RequireSetuper
}

func NewRequire2Module(requireSetuper RequireSetuper) *require2 {
	return &require2{requireSetuper}
}

func (r *require2) SetGlobal(L *lua.LState) {
	L.SetGlobal("require2", L.NewFunction(r.require2))
}

func (r *require2) require2(L *lua.LState) int {
	name := L.ToString(1)

	if r.RequireSetuper != nil {
		newName, err := r.RequireSetuper.Setup(L, name)
		if err != nil {
			L.ArgError(1, fmt.Sprintf("failed call the setup func", err.Error()))
			return 0
		}
		name = newName
	}

	// Now call the original require and return its return value
	if err := L.CallByParam(lua.P{
		Fn:      L.GetGlobal("require"),
		NRet:    1,
		Protect: true,
	}, lua.LString(name)); err != nil {
		panic(err)
	}
	return 1
}
