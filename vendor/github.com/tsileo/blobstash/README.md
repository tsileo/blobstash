BlobStash
=========

[![Travis](https://img.shields.io/travis/tsileo/blobstash.svg)](https://travis-ci.org/tsileo/blobstash)
&nbsp; &nbsp;[![Go Report Card](https://goreportcard.com/badge/github.com/tsileo/blobstash)](https://goreportcard.com/report/github.com/tsileo/blobstash)
&nbsp; &nbsp;[![License](http://img.shields.io/badge/license-MIT-red.svg?style=flat)](https://raw.githubusercontent.com/tsileo/blobstash/master/LICENSE)

Your personal database.

**Still in early development.**

## Manifesto

BlobStash is at the same time a database and web server.

The web server supports HTTP/2 and can generate you TLS certs on the fly using Let's Encrypt.
You can proxy other applications and gives them free certs at the same time, you can also write apps (using Lua) that lets
you interact with BlobStash's database.
Hosting static content is also an option.
It let you easily add authentication to any app/proxied service.

But BlobStash is primarily a database, here are the primitive data types supported by BlobStash.

### Blobs

The content-addressed blob store (the identifier of a blob is its own hash, the chosen hash function is [BLAKE2b](https://blake2.net/)) is at the heart of everything in BlobStash. Everything permanently stored in BlobStash ends up in a blob.

BlobStash has its own storage engine: [BlobsFile](https://github.com/tsileo/blobsfile), data is stored in an append-only flat file.
All data is immutable, stored with error correcting code for bit-rot protection, and indexed in a temporary index for fast access, only 2 seeks operations are needed to access any blobs.

The blob store supports real-time replication via an Oplog (powered by Server-Sent Events) to replicate to another BlobStash instance (or any system), and also support efficient synchronisation between instances using a Merkle tree to speed-up operations.

Asynchronous replication of encrypted blobs to Amazon S3 is supported.


### Key-values

Key-values are the way to keep a mutable reference to an internal or external object, it can be a hash or any sequence of bytes.

It acts like a traditional key-value store, you can set/retrieve/list keys, except for the "version" support.

Each key-value has a timestamp associated, its version. you can easily list all the versions, by default, the latest version is returned.
Internally, each "version" is stored as a separate blob, with a specific format, so it can be detected and re-indexed.

Key-Values are indexed in a temporary database (that can be rebuilt at any time by scanning all the blobs) and stored as a blob.


### JSON documents

A Lua-powered JSON document store let's you perform powerful queries on JSON documents, stored as blobs.

You can easily reference/embed blob or files.

Internally, each document gets a key-value entry, keeping track of the modification history and documents are stored as raw blobs.

When performing queryies, the embedded Lua interpreter runs through all documents sequentially, and returns you the results (indexes support is on its way).

The document store supports ETag, conditional requests (`If-Match`...) and [JSON Patch](http://jsonpatch.com/) for partial/consistent update.

Complex queries can be stored along with the server to prevent wasting bandwith.

### Files, tree of files

Files and tree of files are first-class citizen in BlobStash.

Files are split in multiple chunks (stored as blobs, using content-defined chunking, giving deduplication at the file level), and everything is stored in a kind of Merkle tree where the hash of the JSON file containing the file metadata is the final identifier (which will also be stored as blob).

The JSON format also allow to model directory. A regular HTTP multipart endpoint can convert file to BlobStash internal format for you, or you can do it locally to prevent sending blobs that are already present.

Files can be streamed easily, range requests are supported, EXIF metadata automatically extracted and served, and on-the-fly resizing (with caching) for images.

You can also enable a S3 compatible gateway to manage your files.

## Contribution

Pull requests are welcome but open an issue to start a discussion before starting something consequent.

Feel free to open an issue if you have any ideas/suggestions!

## Donation

[![Flattr this git repo](http://api.flattr.com/button/flattr-badge-large.png)](https://flattr.com/submit/auto?user_id=tsileo&url=https%3A%2F%2Fgithub.com%2Ftsileo%2Fblobstash)

## License

Copyright (c) 2014-2018 Thomas Sileo and contributors. Released under the MIT license.
