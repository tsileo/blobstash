# Lua Extension

You can create **app**, custom API endpoint running [Lua](http://www.lua.org/) script.

You can't access the document store within Lua app (from now at least), only the Blob store and the Key-Value Store.

## External modules available

- `json`: see https://github.com/layeh/gopher-json
- `http`: see https://github.com/cjoudrey/gluahttp

## Examples

### Hello World

```lua
local resp = require('response')

resp.write('Hello World')
```

### Requiring authentication

You can still require authentication on a public app.

```lua
local resp = require('response')
local req = require('request')
local log = require('logger')

local auth = req.authorized()
log.info(string.format("authorized=%s", auth))

if auth then
  resp.write('ok')
else
  resp.authenticate('my app')
  resp.error(401)
end
```

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

// Render execute a Go template, the data must be JSON encoded
render(string, string) -> string
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

### BlobStore

```lua
local bs require('blobstore')

local blob = "data"
local hash = blake2b(blob)

bs.put(hash, blob)

local blob2 = bs.get(hash)
```

```c
// Upload the blob data, first argument is the `blake2b` hash of the blob
put(string, string)

// Retrieve the blob for the given `blake2b` hash
get(string) -> string
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
