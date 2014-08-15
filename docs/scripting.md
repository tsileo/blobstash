# Scripting

You can extend BlobStash by running [Lua](http://www.lua.org/) program that can create transaction and/or read data.

The Lua program must returns an associative array (a table).

## Helpers available

In addition the standard [Luar helpers](https://github.com/stevedonovan/luar) (like ``luar.map2table``), you have access to:

- ``blake2b()`` to compute the blake2b hash.
- ``blobstash.Tx`` to write data ([API docs here](http://godoc.org/github.com/tsileo/blobstash/client/transaction))
- ``blobstash.DB`` to query data
- ``blobstash.Args`` the aguments provided in the POST request (**_args**)

### blobstash.DB

#### blobstash.DB.Get

#### blobstash.DB.Smembers

#### blobstash.DB.Llast

#### blobstash.DB.GetHash

Returns the hash as a Lua table (map[string]string).

There is one drawback: **you must cast int/float/bool value server-side**.

```Lua
local myhash, _ = blobstash.DB.GetHash("myhash")
return {res = myhash.mykey}
```

## Usage

You must send a POST request at **http://localhost:9736/scripting** with a JSON object contanining the following keys:

- **_args**: can be anything, will be accessible within the LUA script under ``blobstash.Args``
- **_script**: the [Lua](http://www.lua.org/) script as string

### Example

```console
$ curl -X POST http://localhost:9736/scripting -d '{"_script": "return {Hello = \"World\"}", "_args": {}}'
{"Hello":"World"}
```

### Testing

You can test Lua script directly in your browser [http://localhost:9736/debug/scripting](http://localhost:9736/debug/scripting).

### Go client

See the [godoc documentation for client/script](http://godoc.org/github.com/tsileo/blobstash/client/script).
