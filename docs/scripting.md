# Scripting

You can extend BlobStash by running [Lua](http://www.lua.org/) program that can create transaction and/or read data.

```console
$ curl -X POST http://localhost:9736/scripting -d '{"_script": "return {Hello = \"World\"}", "_args": {}}'
{"Hello":"World"}
```

The Lua program must returns an associative array (a table).
