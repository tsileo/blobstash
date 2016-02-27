BlobStash
=========

**BlobStash** is both a content-addressed blob store and a key value store accessible via an HTTP API.

Key value pairs are stored as "meta" blobs, this mean you can build application on top of BlobStash without the need for another database.

**Still in early development.**

## Manifesto

You can store all your life's data in BlobStash, from raw blobs to full file systems.

To store your data and build app, you can use a combination of:

- The Blob store
- The Key-Value store
- The JSON document store
- Build Lua app that runs inside BlobStash, will be accessible over HTTP and can access all the previous APIs.

Everything is private by default, but can support public and semi-private sharing (via Lua scripting and Hawk bewit).

## Projects built on top of BlobStash

 - [BlobFS](https://github.com/tsileo/blobfs)

Make a pull request if your project uses BlobStash as data store or if you built an open-source Lua app for BlobStash.

## Features

- All data you put in it is deduplicated (thanks to content-addressing).
- Create app with a powerful Lua API (like OpenResty)
- Optional encryption (using [go.crypto/nacl secretbox](http://godoc.org/code.google.com/p/go.crypto/nacl))
- [BLAKE2b](https://blake2.net) as hashing algorithm for the content-addressed blob store
- Backend routing, you can define rules to specify where blobs should be stored ("if-meta"...)
- TLS and HTTP2 support
- A full featured Go [client](http://godoc.org/github.com/tsileo/blobstash/client) using the HTTP API
- Can be embedded in your Go program ([embedded client](http://godoc.org/github.com/tsileo/blobstash/embed))

## Getting started

```console
$ go get github.com/tsileo/blobstash/cmd/blobstash
$ $GOPATH/bin/blobstash
INFO[02-25|00:05:40] Starting blobstash version 0.0.0; go1.6beta1 (darwin/amd64)
INFO[02-25|00:05:40] opening blobsfile for writing            backend=blobsfile-/Users/thomas/var/blobstash/blobs
INFO[02-25|00:05:40] server: HTTP API listening on 0.0.0.0:8050
```

## Blob store

You can deal directly with blobs when needed using the HTTP API, full docs [here](docs/blobstore.md).

```console
$ curl -F "c0f1480a26c2fd4deb8e738a52b7530ed111b9bcd17bbb09259ce03f129988c5=ok" http://0.0.0.0:8050/api/v1/blobstore/upload
```

## Key value store

Updates on keys are store in blobs, and automatically handled by BlobStash.

Perfect to keep a mutable pointer.

```console
$ curl -XPUT http://127.0.0.1:8050/api/v1/vkv/key/k1 -d value=v1
{"key":"k1","value":"v1","version":1421705651367957723}
```

```console
$ curl http://127.0.0.1:8050/api/v1/vkv/key/k1
{"key":"k1","value":"v1","version":1421705651367957723}
```

## Extensions

BlobStash comes with few bundled extensions making it easier to build your own app on top of it.

Extensions only uses the blob store and the key value store, nothing else.

### Document Store

A JSON document store running on top of an HTTP API. Support a subset of the MongoDB Query language.

JSON documents are stored as blobs and the key-value store handle the indexing.

Perfect for building app designed to only store your own data.

See [here for more details](docs/docstore.md).

#### Client

- [docstore-client written in Go](https://github.com/tsileo/docstore-client).

### Lua App/Scripting

You can create **app**, custom API endpoint running [Lua](http://www.lua.org/) script (like OpenResty).

See the [Lua API here](docs/lua.md).

#### Examples

 - [BlobsBin](https://github.com/tsileo/blobsbin), a pastebin like service.
 - [ ] A Markdown-powered blog app
 - [ ] Sharing script
 - [ ] Lua iCal feed script
 - [ ] IoT data store (temp/humid with avg)
 - [ ] Pebble app backend example

## Backend

Blobs are stored in a backend.

The backend handle operations:

- Put
- Exists
- Get
- Enumerate

Delete is not implemented for all backends.

### Available backends

- [BlobsFile](docs/blobsfile.md) (local disk, the preferred backend)
- AWS S3 (useful for secondary backups)
- Mirror (mirror writes to multiple backend)
- A remote BlobStash instance (working, bot full-featured)
- Fallback backend (store failed upload locally and try to re-upload them periodically)
- AWS Glacier (only as a backup, **development paused**)

- Submit a pull request!

You can combine backend as you wish, e.g. Mirror( Encrypt( S3() ), BlobsFile() ).

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

## Embedded mode

```go
package main

import (
	"github.com/tsileo/blobstash/server"
)

func main() {
	blobstash := server.New(nil)
	blobstash.SetUp()
	// wait till all meta blobs get scanned
	blobstash.TillReady()
	bs := blobstash.BlobStore()
	kvs := blobstash.KvStore()
	blobstash.TillShutdown()
}
```

## Roadmap / Ideas

- [ ] Bind a Lua app to root (`/`)
- [ ] A `blobstash-sync` subcommand
- [ ] Fine grained permission for the document store
- [ ] A File extension with tree suport (files as first-class citizen)
- [ ] Display mutation history for the docstore document (`/{doc _id}/history`)
- [ ] A lua module for nacl box?
- [X] A Lua module for the document store
- [ ] Find a way to handle/store? app logs
- [X] A better template module for Lua app -> load a full directory as an app
- [ ] Integrate with Let's Encrypt (via lego)
- [X] Snappy encoding support for the HTTP blobstore API
- [ ] A slave blobstash mode (e.g. for blog/remote apps)
- [X] A Lua LRU module
- A better documentation
- A web interface?
- An S3-like HTTP API to store archive?
- Fill an issue!

## Contribution

Pull requests are welcome but open an issue to start a discussion before starting something consequent.

Feel free to open an issue if you have any ideas/suggestions!

## Donation

[![Flattr this git repo](http://api.flattr.com/button/flattr-badge-large.png)](https://flattr.com/submit/auto?user_id=tsileo&url=https%3A%2F%2Fgithub.com%2Ftsileo%2Fblobstash)

BTC 12XKk3jEG9KZdZu2Jpr4DHgKVRqwctitvj

## License

Copyright (c) 2014-2015 Thomas Sileo and contributors. Released under the MIT license.
