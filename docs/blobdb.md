# BlobDB

**BlobDB** is a database build on top of a content-addressable blob store, it includes:

- a HTTP blob store, for get/put/exists operations on blob.
- a Redis-like data structure server with custom data type (data is stored in blobs), compatible the with [Redis Protocol](http://redis.io/topics/protocol).

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

## Routing

You can define rules to specify where blobs should be stored, depending on whether it's a meta blob or not, or depending on the host it come from.

**Blobs are routed to the first matching rule backend, rules order is important.**

```json
[
    [["if-host-tomt0m", "if-meta"], "customHandler2"],
    ["if-host-tomt0m", "customHandler"],
    ["if-meta", "metaHandler"],
    ["default", "blobHandler"]
]
```

The minimal router config is:

```json
[["default", "blobHandler"]]
```
