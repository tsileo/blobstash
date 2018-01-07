package gluarequire2

import (
	"testing"

	"github.com/yuin/gopher-lua"
)

func TestRequireFromGitHub(t *testing.T) {
	L := lua.NewState()
	defer L.Close()

	NewRequire2Module(NewRequireFromGitHub(nil)).SetGlobal(L)

	if err := L.DoString(`
	local testmod = require2('github.com/tsileo/gluarequire2/_tests/testmod')
	assert(testmod.return1() == 1)
	`); err != nil {
		panic(err)
	}

	// Do it a second time, this time, the code should be already cached
	if err := L.DoString(`
	local testmod = require2('github.com/tsileo/gluarequire2/_tests/testmod')
	assert(testmod.return1() == 1)
	`); err != nil {
		panic(err)
	}

	// Test nested import (testmod2 require testmod using require2)
	if err := L.DoString(`
	local testmod2 = require2('github.com/tsileo/gluarequire2/_tests/testmod2')
	assert(testmod2.return2() == 2)
	`); err != nil {
		panic(err)
	}
}

func TestRequireNoop(t *testing.T) {
	L := lua.NewState()
	defer L.Close()

	NewRequire2Module(nil).SetGlobal(L)

	if err := L.DoString(`
	local testmod = require2('_tests/testmod')
	assert(testmod.return1() == 1)
	`); err != nil {
		panic(err)
	}
}
