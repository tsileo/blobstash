# Lua Extension

You can create **app**, custom API endpoint running [Lua](http://www.lua.org/) script.

You can't access the document store within Lua app (from now at least), only the Blob store and the Key-Value Store.

You can access some built-in Lua modules/functions like `string` or `pcall`.

## External modules available

- `json`: see https://github.com/layeh/gopher-json
- `http`: see https://github.com/cjoudrey/gluahttp

## Constant available

- `reqID`: the unique request ID
- `appID`: the current script appID

```lua
local resp = require('response')

resp.write(string.format("Hello from %s", appID))
```

## Examples

### Hello World

```lua
local resp = require('response')

resp.write('Hello World')
```

### File upload

```lua
local req = require('request')
local log = require('logger')
values, files = req.upload()

for key, val in pairs(values) do
  log.info(string.format("key=%q,value=%q", key, val))
end

for key, val in pairs(files) do
  log.info(string.format("key=%q,value=%q,mimetype=%q,size=%d", key, val.filename, val.mimeType, val.size))
  -- you can access the file content with `val.content`
end
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

// Generate a random hexadecimal ID with the current timestamp as first 4 bytes,
// this means keys will be sorted by creation date automatically if sorted lexicographically
hexid() -> string

// Compute the Blake2B hash for the given string
blake2b(string) -> string

// Sleep for the given number of seconds
sleep(number)

// Convert the given Markdown to HTML
markdownify(string) -> string

// Render execute a Go HTML template, data must be a table with string keys
render(string, string) -> string

// Build an URL using the server hostname
url(string) -> string
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

// Return the request URL
url() -> string

// Return the host component
host() -> string

// Return the path component of the URL
path() -> string

// Return the HTTP header for the given key
header(string) -> string

// Return all the HTTP headers as a table
headers() -> table

// Return the HTTP request body as a string
body() -> string

// Return the HTTP request body parsed as JSON
json() -> table/string/number...

// Return the form-encoded data as a Lua table
formdata() -> table

// Return the query argument for the given key
queryarg(string) -> string

// Return the query arguments as a Lua table
queryargs() -> table

// Return a boolean indicating whether the request is authenticated using a valid API key
authorized() -> bool

// Return true if request contain uploaded files
hasupload() -> bool

// Parse the request and extract a table containing form key-values,
// and a table indexed by uploaded filename returning a table with: filename, size, content key.
upload() -> table, table

// Return the client IP
remoteaddr() -> string

// Return the request URL referer
referer() -> string

// Return the request URL user agent
useragent() -> string
```

### Response

```lua
local resp = require('response')

resp.stats(404)
resp.write('Nothing to see here')
```

```lua
local resp = require('response')

-- Set a custom header
resp.header("My-Custom-Header", "Value")

-- Output JSON with a 200 status code
resp.jsonify{status = "It Works!"}
```

```c
// Set the HTTP status to the given int
status(int)

// Set the HTTP header
header(string, string)

// Write to the output buffer
write(string)

// Output JSON (with the right Content-Type), the data must be a table (or use `json` module with write).
jsonify(string)

// Return an error with the given status code and an optional error message
error(int[, message])

// Set the header for asking Basic Auth credentials (with the given realm)
authenticate(string)
```

### KvStore

```lua
local kvs = require('kvstore')
local log = require('logger')

kvs.put('key', 'value', -1) -- Set the version to the current timestamp

local kv = kvs.get('key', -1) -- Fetch the latest version

log.info(string.format('key=%q,value=%q,version=%d', kv.key, kv.value, kv.version))
```

```c
// Get the key at the given version, -1 means latest version
get(string, int) -> table

// Set the key to value for the given version, -1 means the current timestamp (from server)
put(string, string, int) -> table

// List the keys between start and end (limit results to n)
keys(string, string, int) -> table (list)

// Retrieve all the version start < version < end for the given key (limit results to n)
versions(string, int, int, int) -> table(list)
```

### BlobStore

```lua
local bs = require('blobstore')

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

// Check if the blob exist
stat(string) -> bool

// Return the list of key "start" < key < "end" (limit to n hashes)
enumerate(string, string, int) -> table
```

### Logger

```lua
local log = require('logger')

log.info('script started')

local res = "ok"
log.debug(string.format("res=%q", res))
```

```c
info(string)
debug(string)
error(string)
```
