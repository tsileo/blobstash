BlobStash
=========

**BlobStash** is both a content-addressed blob store and a key value store accessible via an HTTP API.

Key value pairs are stored as "meta" blobs.

Initially created to power [BlobSnap](https://github.com/tsileo/blobsnap).

## Features

- [BLAKE2b](https://blake2.net) as hashing algorithm for the content-addressed blob store
- Backend routing, you can define rules to specify where blobs should be stored ("if-meta"...)
- Optional encryption (using [go.crypto/nacl secretbox](http://godoc.org/code.google.com/p/go.crypto/nacl))
- Possibility to incrementally archive blobs to AWS Glacier (with a recovery command-line tool)
- A full featured Go [client](http://godoc.org/github.com/tsileo/blobstash/client)

## Getting started

```console
$ go get github.com/tsileo/blobstash/cmd/blobstash
$ $GOPATH/bin/blobstash
2014/07/29 19:54:34 Starting blobstash version 0.1.0; go1.4 (linux/amd64)
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
$ curl -F "c0f1480a26c2fd4deb8e738a52b7530ed111b9bcd17bbb09259ce03f129988c5=ok" http://0.0.0.0:8050/api/v1/blobstore/upload
```

## Key value store

Updates on keys are store in blobs, and automatically handled by BlobStash.

```console
$ curl -XPUT http://127.0.0.1:8050/api/v1/vkv/key/k1 -d value=v1
{"key":"k1","value":"v1","version":1421705651367957723}
```

```console
$ curl http://127.0.0.1:8050/api/v1/vkv/key/k1            
{"key":"k1","value":"v1","version":1421705651367957723}
```

## Backend

Blobs are stored in a backend.

The backend handle operations:

- Put
- Exists
- Get
- Delete
- Enumerate

### Available backends

- [BlobsFile](docs/blobsfile.md) (local disk)
- AWS S3
- Mirror
- AWS Glacier (only as a backup)
- A remote BlobDB instance? (not started yet)
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

## Roadmap / Ideas

- A better documentation
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
