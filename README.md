BlobStash
=========

**BlobStash** is an immutable database build on top of a content-addressable blob store, it includes:

- a HTTP blob store, for get/put/exists operations on blob.
- a Redis-like data structure server with custom immutable data type (transactions are stored in blobs), compatible the with the [Redis Protocol](http://redis.io/topics/protocol).

Initially created to power [BlobSnap](https://github.com/tsileo/blobsnap) and [BlobPad](https://github.com/tsileo/blobpad).

**Still in early development, but I expect to release a v0.1.0 soon.**

## Features

- [BLAKE2b](https://blake2.net) as hashing algorithm for the blob store
- Immutability reduce the risk of loosing data
- A full featured [Go client](http://godoc.org/github.com/tsileo/blobstash/client)
- Backend routing with namespacing, you can define rules to specify where blobs should be stored ("if-meta", "if-ns-myhost"...) and setup custom context
- [Lua](http://www.lua.org/) scripting support
- Optional encryption (using [go.crypto/nacl secretbox](http://godoc.org/code.google.com/p/go.crypto/nacl))
- Possibility to incrementally archive blobs to AWS Glacier (with a recovery command-line tool)

Draws inspiration from [Camlistore](http://camlistore.org/) and [bup](https://github.com/bup/bup) (files are split into multiple blobs using a rolling checksum).

## Getting started

```console
$ sudo apt-get install liblua5.1-dev
$ go get github.com/tsileo/blobstash/cmd/blobstash
$ $GOPATH/bin/blobstash
2014/07/29 19:54:34 Starting blobstash version 0.1.0; go1.3 (linux/amd64)
2014/07/29 19:54:34 BlobsFileBackend: starting, opening index
2014/07/29 19:54:34 BlobsFileBackend: scanning BlobsFiles...
2014/07/29 19:54:34 BlobsFileBackend: opening /home/thomas/var/blobstash/blobs/blobs-00000 for writing
2014/07/29 19:54:34 BlobsFileBackend: running fallocate on BlobsFile /home/thomas/var/blobstash/blobs/blobs-00000
2014/07/29 19:54:34 BlobsFileBackend: snappyCompression = true
2014/07/29 19:54:34 BlobsFileBackend: backend id => blobsfile-/home/thomas/var/blobstash/blobs
2014/07/29 19:54:34 server: http server listening on http://:9736
2014/07/29 19:54:34 server: listening on tcp://:9735
2014/07/29 19:54:34 scanning meta blobs blobsfile-/home/thomas/var/blobstash/blobs
2014/07/29 19:54:34 scan result for blobsfile-/home/thomas/var/blobstash/blobs: {Blobs:0 MetaBlobs:0 Applied:0 Size:0}
```

## Blob store

You can deal directly with blobs when needed using the HTTP API, full docs [here](docs/blobstore.md).

```console
$ curl -H "BlobStash-Namespace: mynamespace" -F "c0f1480a26c2fd4deb8e738a52b7530ed111b9bcd17bbb09259ce03f129988c5=ok" http://0.0.0.0:9736/upload
```

## Data structure server

BlobStash implements 4 immutable data types (no in-place update/delete features by design):

- Strings (GET/SET)
- Sets (SADD/SMEMBERS/SCARD)
- Hashes (HMSET/HLEN/HGET/HGETALL/HSCAN)
- Indexed lists (LADD/LITER/LRANGE/LLEN/LINDEX)

Full commands list [here](docs/commands.md).

The database can only be updated within a transaction (TXINIT/TXCOMMIT),
every request will be added to a ReqBuffer, and on commit, it will be dumped to JSON and saved as blob,
more info [in the docs directory](docs/under-the-hood.md).

BlobStash keeps an index used for querying, at startup all blobs are scanned and meta blobs are applied if needed.

### Talks to the DB with using Redis protocol

You can inspect the database with any Redis-compatible client.

```console
$ redis-cli -p 9736
127.0.0.1:9736> ping
PONG
127.0.0.1:9736> 
```

## Backend

Blobs are stored in a backend.

The backend handle operations:

- Put
- Exists
- Get
- Enumerate

You can combine backend as you wish, e.g. Mirror( Encrypt( S3() ), BlobsFile() ).

### Available backends

- [BlobsFile](docs/blobsfile.md) (local disk)
- AWS S3
- Mirror
- AWS Glacier (only as a backup)
- A remote BlobDB instance? (not started yet)
- Submit a pull request!

## Namespace

When interacting with BlobDB, you must specify a **namespace**, used to indicate ownership, like database.

## Routing

You can define rules to specify where blobs should be stored, depending on whether it's a meta blob or not, or depending on the namespace it come from.

**Blobs are routed to the first matching rule backend, rules order is important.**

```json
[
    [["if-ns-myhost", "if-meta"], "customHandler2"],
    ["if-ns-myhost", "customHandler"],
    ["if-meta", "metaHandler"],
    ["default", "blobHandler"]
]
```

The minimal router config is:

```json
[["default", "blobHandler"]]
```

## Scripting

You can extend BlobStash by running [Lua](http://www.lua.org/) program that can create transaction and/or read data.

```console
$ curl -X POST http://localhost:9736/scripting -d '{"_script": "return {Hello = \"World\"}", "_args": {}}'
{"Hello":"World"}
```

The Lua program must returns an associative array (a table), more docs [here](docs/scripting.md).

## Roadmap / Ideas

- A web interface
- An S3-like HTTP API to store archive
- Fill an issue!

## Contribution

Pull requests are welcome but open an issue to start a discussion before starting something consequent.

Feel free to open an issue if you have any ideas/suggestions!

## Donation

[![Flattr this git repo](http://api.flattr.com/button/flattr-badge-large.png)](https://flattr.com/submit/auto?user_id=tsileo&url=https%3A%2F%2Fgithub.com%2Ftsileo%2Fblobstash)

BTC 1HpHxwNUmXfrU9MR9WTj8Mpg1YUEry9MF4

## License

Copyright (c) 2014 Thomas Sileo and contributors. Released under the MIT license.
