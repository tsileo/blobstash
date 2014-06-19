BlobStash
=========

BlobStash is a snapshot-based backup system, designed to provide "time machine" like features.

**Designed with Linux in mind**.

## Features

- Content addressed, files are split into blobs, and retrieved by hash
- Incremental backups/snapshots thanks to data deduplication
- Server handles uploading/downloading blobs to/from different storage
- Client only query the server and send blobs to it (the client take care of chunking/building blobs)
- Read-only FUSE file system to navigate backups/snapshots
- Optional encryption (using [go.crypto/nacl secretbox](http://godoc.org/code.google.com/p/go.crypto/nacl))
- Take snapshot automatically every x minutes, using a separate client-side scheduler (provides Arq/time machine like backup)
- Possibility to incrementally archive blobs to AWS Glacier (with a recovery command-line tool)
- Strong test suite (unit tests + integration tests)
- Support for backing-up multiple hosts (you can force a different host to split backups into "different buckets")
- Backed routing, you can define rules to specify where blobs should be stored ("if-meta", "if-host-myhost"...)

Draws inspiration from [Camlistore](camlistore.org) and [bup](https://github.com/bup/bup) (files are split into multiple blobs using a rolling checksum).

## Components

### Database

**BlobDB** is a backup database (a Content-Addressable Storage and a data structure server) designed to efficiently handle snapshots of files/directories, built on top of [kv](https://github.com/cznic/kv) and the [Redis Protocol](http://redis.io/topics/protocol).

#### Backend

- BlobsFile (local disk)
- AWS S3
- Mirror
- AWS Glacier
- A remote BlobDB instance? (not started yet)
- Submit a pull request!

##### Router

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

### Fuse file system

**BlobFS** is the most convenient way to restore/navigate snapshots is the FUSE file system.

There is three magic directories at the root:

- **latest**: it contains the latest version of every snapshots/backups.
- **snapshots**: it let you navigate for every snapshots, you can see every versions.
- **at**: let access directories/files at a given time, it automatically retrieve the closest previous snapshots.

```console
$ blobstash mount /backups
2014/05/12 17:26:34 Mounting read-only filesystem on /backups
Ctrl+C to unmount.
```

```console
$ ls /backups
tomt0m
$ ls /backups/tomt0m
at  latest  snapshots
$ ls /backups/tomt0m/latest
writing
$ ls /backups/tomt0m/snapshots/writing
2014-05-11T11:01:07+02:00  2014-05-11T18:36:06+02:00  2014-05-12T17:25:47+02:00
$ ls /backups/tomt0m/at/writing/2014-05-12
file1  file2  file3
```
### Command-line client

**blobstash** is the command-line client to perform/restore snapshots/backups.

```console
$ blobstash put /path/to/dir/or/file
```

### Backup scheduler

The backup scheduler allows you to perform snapshots...

## How it works

### Backend

A **backend** handle blobs operation (blobsfile/s3/encrypt/mirror/remote).

- Put
- Exists
- Get
- Enumerate

You can combine backend as you wish, e.g. Mirror( Encrypt( S3() ), BlobsFile() ).

## Roadmap / Ideas

- an Android app to backup Android devices
- Follow .gitignore file
- Garbage collection (sparse files support for blob files)
- A web interface
- Fill an issue!

## Donate!

[![Flattr this git repo](http://api.flattr.com/button/flattr-badge-large.png)](https://flattr.com/submit/auto?user_id=tsileo&url=https%3A%2F%2Fgithub.com%2Ftsileo%2Fblobstash)

BTC 1HpHxwNUmXfrU9MR9WTj8Mpg1YUEry9MF4
