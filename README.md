BlobStash
=========

**BlobStash** is an immutable database build on top of a content-addressable ([BLAKE2b](https://blake2.net) hash) blob store, it includes:

- a HTTP blob store, for get/put/exists operations on blob.
- a Redis-like data structure server with custom immutable data type (transactions are stored in blobs), compatible the with the [Redis Protocol](http://redis.io/topics/protocol).

## Features

- Optional encryption (using [go.crypto/nacl secretbox](http://godoc.org/code.google.com/p/go.crypto/nacl))
- Possibility to incrementally archive blobs to AWS Glacier (with a recovery command-line tool)
- Strong test suite (unit tests + integration tests)
- Backend routing with namespacing, you can define rules to specify where blobs should be stored ("if-meta", "if-ns-myhost"...) and setup custom context

Draws inspiration from [Camlistore](camlistore.org) and [bup](https://github.com/bup/bup) (files are split into multiple blobs using a rolling checksum).

## Blob store

You can deal directly with blob when needed using the HTTP API:

```console
$ curl -H "BlobStash-Hostname: ok2" -H "Blobstash-Meta: 0" -F "92a949fd41844e1bb8c6812cdea102708fde23a4=ok" http://0.0.0.0:9736/upload
```

## Data structure server

BlobDB implements 4 immutable data types (no update/delete features by design):

- Strings (GET/SET)
- Sets (SADD/SMEMBERS/SCARD)
- Hashes (HMSET/HLEN/HGET/HGETALL/HSCAN)
- Indexed lists (LADD/LITER/LRANGE/LLEN/LINDEX)

The database can only be updated during a transcation (TXINIT/TXCOMMIT),
every request will be added to a ReqBuffer, and on commit, it will be dumped to JSON and saved as blob.

BlobDB keep an index used for querying.

## Backend

Blobs are stored in a backend.

The backend handle operations:

- Put
- Exists
- Get
- Enumerate

You can combine backend as you wish, e.g. Mirror( Encrypt( S3() ), BlobsFile() ).

### Available backends

- BlobsFile (local disk)
- AWS S3
- Mirror
- AWS Glacier
- A remote BlobDB instance? (not started yet)
- Submit a pull request!

## Namespace

When interacting with BlobDB, you must specify a **namespace**, used to indicate ownership,

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

## Roadmap / Ideas

- A web interface
- An S3-like HTTP API to store archive
- Fill an issue!

## Donate!

[![Flattr this git repo](http://api.flattr.com/button/flattr-badge-large.png)](https://flattr.com/submit/auto?user_id=tsileo&url=https%3A%2F%2Fgithub.com%2Ftsileo%2Fblobstash)

BTC 1HpHxwNUmXfrU9MR9WTj8Mpg1YUEry9MF4
