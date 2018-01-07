# Gluapp

[![Build Status](https://travis-ci.org/tsileo/gluapp.svg?branch=master)](https://travis-ci.org/tsileo/gluapp)
&nbsp; &nbsp;[![Godoc Reference](https://godoc.org/a4.io/gluapp?status.svg)](https://godoc.org/a4.io/gluapp)

HTTP framework for [GopherLua](https://github.com/yuin/gopher-lua).

## Features

 - Simple
 - No 3rd party requirements except gopher-lua
 - Rely on Go template language
 - Same request/response idioms as Go HTTP lib
 - Comes with a basic (and optional) router
 - First-class JSON support
 - Included HTTP client
 - Support importing dependency from GitHub on the fly with [require2](https://github.com/tsileo/gluarequire2)

## Example

```lua
local router = require('router').new()

router:get('/hello/:name', function(params)
  app.response:write('hello ' .. params.name)
end)

router:run()
```

## TODO

 - [ ] Write Lua modules documentation
 - [ ] A module for web scrapping
 - [ ] A basic key-value store module
 - [ ] `read_file`/`write_file`/`read_json` helper
