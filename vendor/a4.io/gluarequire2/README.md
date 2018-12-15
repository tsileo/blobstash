# gluarequire2

[![Build Status](https://travis-ci.org/tsileo/gluarequire2.svg?branch=master)](https://travis-ci.org/tsileo/gluarequire2)
&nbsp; &nbsp;[![Godoc Reference](https://godoc.org/a4.io/gluarequire2?status.svg)](https://godoc.org/a4.io/gluarequire2)

gluarequire2 provides a way to import file directly from GitHub using [GopherLua](https://github.com/yuin/gopher-lua).

Files are downloaded on the fly via HTTP (and stored in `/tmp`), then `require2` rewrite the imports to the right location.

## Installation

```shell
$ go get a4.io/gluarequire2
```

### QuickStart

```go
package main

import (
	"a4.io/gluarequire2"
	"github.com/yuin/gopher-lua"
)

func main() {
	L := lua.NewState()
	defer L.Close()

	gluarequire2.NewRequire2Module(gluarequire2.NewRequireFromGitHub(nil)).SetGlobal(L)

	if err := L.DoString(`

        local mymod = require2('github.com/tsileo/gluarequire2/_tests/testmod')
	assert(mymod.return1() == 1)

    `); err != nil {
		panic(err)
	}
}
```
