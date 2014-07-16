# BlobDB

**BlobDB** is a database build on top of a content-addressable blob store, it includes:

- a HTTP blob store, for get/put/exists operations on blob.
- a Redis-like data structure server with custom data type (data is stored in blobs), compatible the with [Redis Protocol](http://redis.io/topics/protocol).

## Blob store

You can deal directly with blob when needed using the HTTP API:

$ curl -H "BlobStash-Hostname: ok2" -H "Blobstash-Meta: 0" -F "92a949fd41844e1bb8c6812cdea102708fde23a4=ok" http://0.0.0.0:9736/upload


## Data structure server

Y

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
