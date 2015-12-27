# Lua Extension

You can create custom API endpoint running [Lua](http://www.lua.org/) your script.

## API

### Globals

These functions are available globally.

```c
// Return the server unix timestamp
unix() -> number

// Compute the Blake2B hash for the given string
blake2b(string) -> string

// Sleep for the given number of seconds
sleep(number)

// Convert the given Markdown to HTML
markdownify(string) -> string
```

### Request

The `request` module let you interact with the incoming HTTP request.

```lua
local req = require('request')
local log = require('logger')

log.info(string.format("method=%s", req.method()))
```

```c
// Return the HTTP method (GET, POST...)
method() -> string

// Return the HTTP header for the given key
header(string) -> string

// Return all the HTTP headers as a table
headers() -> table

// Return the HTTP request body as a string (and an error if any). Can only be called once.
body() -> string, string

// Return the form-encoded data as a Lua table
formdata() -> table

// Return the query argument for the given key
queryarg(string) -> string

// Return the query arguments as a Lua table
queryargs() -> table
```

### Response

```lua
local resp = require('response')

resp.stats(404)
resp.write('Nothing to see here')
```

```c
// Set the HTTP status to the given int
status(int)

// Set the HTTP header
header(string, string)

// Write to the output buffer
write(string)

// Output JSON, the payload must already be JSON encoded
writejson(string)
```

### Logger

```lua
local log = require('logger')

log.info('script started')
```

```c
info(string)
debug(string)
```
