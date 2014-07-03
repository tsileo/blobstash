# Under the hood

## Talks to the DB with using Redis protocol

You can inspect the database with any Redis-compatible client:

```console
$ redis-cli -p 9736
127.0.0.1:9736> ping
PONG
127.0.0.1:9736> 
```

## Monitor commands

You can monitor the database using the Server-Sent Events API:

```console
$ curl http://0.0.0.0:9737/debug/monitor
data: hgetall command  with args [2c07e10cd475290b49f5f943315b65f6a16192b5] from client: 127.0.0.1:42179

data: hgetall command  with args [92d430a8dbe6a6ace8db423d231ead6a4bf33634] from client: 127.0.0.1:42179

```

## Debug variables

You can check debug vars via the [expvar](http://golang.org/pkg/expvar/) interface:

```console
$ curl http://0.0.0.0:9737/debug/vars
{
  "blobsfile-blobs-downloaded": {}, 
  "blobsfile-blobs-uploaded": {
    "/box/blobsfileconfigcreatedmeta2": 1
  }, 
  "blobsfile-bytes-downloaded": {}, 
  "blobsfile-bytes-uploaded": {
    "/box/blobsfileconfigcreatedmeta2": 472
  }, 
  "blobsfile-open-fds": {
    "/box/blobsfileconfigcreatedblobs2": 2, 
    "/box/blobsfileconfigcreatedmeta2": 2
  }, 
  "cmdline": [
    "/tmp/go-build248426472/command-line-arguments/_obj/exe/server"
  ], 
  "encrypt-blobs-downloaded": {}, 
  "encrypt-blobs-uploaded": {}, 
  "encrypt-bytes-downloaded": {}, 
  "encrypt-bytes-uploaded": {}, 
  "glacier-blobs-downloaded": {}, 
  "glacier-blobs-uploaded": {}, 
  "glacier-bytes-downloaded": {}, 
  "glacier-bytes-uploaded": {}, 
  "memstats": {
    [...]
  }, 
  "mirror-blobs-downloaded": {}, 
  "mirror-blobs-uploaded": {}, 
  "mirror-bytes-downloaded": {}, 
  "mirror-bytes-uploaded": {}, 
  "server-active-monitor-client": 0, 
  "server-command-stats": {
    "done": 1, 
    "hgetall": 76, 
    "hlen": 24, 
    "hmset": 1, 
    "init": 1, 
    "ladd": 1, 
    "llast": 2, 
    "llen": 36, 
    "sadd": 1, 
    "scard": 20, 
    "smembers": 4, 
    "txcommit": 1, 
    "txinit": 61
  }, 
  "server-started-at": "12 Jun 14 20:11 +0000", 
  "server-total-connections-received": 22
}
```

## Metadata format

Metadata are stored in in a kv file and are exposed via a Redis protocol tcp server, with custom Redis-like data type and commands, but implemented using kv lexicographical range queries.

- String data type
- Hash data type
- Set (lexicographical order) data type (used to store hash list)
- List (sorted by an uint index) data type
- "Virtual" Blob data type (upload/download from/to storage)

Check [db/buffer.go](https://github.com/tsileo/blobstash/blob/master/server/buffer.go) for more documentation.

A backup is a set with pointer to hash (either representing a directory or a file, and a directory is also a set of pointer).

If a file is stored multiple times, metadata are not duplicated.

A hash contains the backup parts reference, an ordered list of the files hash blobs.

Metadata are stored in meta blobs.

## Meta blobs

A meta blobs contains a set of DB operations.

### Meta blobs creation

There is two way to create meta blobs:

- Create the meta blob client-side and uploading it once finished (with a ``MBPUT`` command).
- Via a transaction, the server store commands and automatically create and upload the meta blob once the transaction is committed (with ``TXINIT $SERVER $ARCHIVE_MODE``/``TXCOMMIT``.
