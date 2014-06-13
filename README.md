Data Database
=============

## Overview

A set of backup tools designed to provides a "time machine" like experience:

- a database (content-addressed store + data structure server)
- a backup scheduler (cron/anacron like)
- a read-only FUSE file system
- a command-line client
- a web application (on the roadmap)

**Designed with Linux in mind**.

## Features:

- Content addressed, files are split into blobs, and retrieved by hash
- Incremental backups/snapshots thanks to data deduplication
- Server handles uploading/downloading blobs to/from different storage
- Client only query the server and send blobs to it (the client take care of chunking/building blobs)
- Read-only FUSE file system to navigate backups/snapshots
- Optional encryption (using [go.crypto/nacl secretbox](http://godoc.org/code.google.com/p/go.crypto/nacl))
- Take snapshot automatically every x minutes, using a separate client-side daemon (provides Arq/time machine like backup)
- Deletion/garbage collection isn't implemented yet, but it's on the roadmap
- Possibility to archive blobs to AWS Glacier (with a recovery command-line tool)

Draws inspiration from [Camlistore](camlistore.org) and [bup](https://github.com/bup/bup) (files are split into multiple blobs using a rolling checksum).

## Components

### Database

**Datadb** is a backup database (a Content-Addressable Storage and a data structure server) designed to efficiently handle snapshots of files/directories, built on top of [kv](https://github.com/cznic/kv) and the [Redis Protocol](http://redis.io/topics/protocol).

#### Backend

- BlobsFile (local disk)
- AWS S3
- Mirror
- AWS Glacier
- A remote DataDB instance? (not started yet)
- Submit a pull request!

### Fuse file system

The most convenient way to restore/navigate snapshots is the FUSE file system.

There is three magic directories at the root:

- **latest**: it contains the latest version of every snapshots/backups.
- **snapshots**: it let you navigate for every snapshots, you can see every versions.
- **at**: let access directories/files at a given time, it automatically retrieve the closest previous snapshots.

```console
$ datadb mount /backups
2014/05/12 17:26:34 Mounting read-only filesystem on /backups
Ctrl+C to unmount.
```

```console
$ ls /backups
at  latest  snapshots
$ ls /backups/latest
writing
$ ls /backups/snapshots/writing
2014-05-11T11:01:07+02:00  2014-05-11T18:36:06+02:00  2014-05-12T17:25:47+02:00
$ ls /backups/at/writing/2014-05-12
file1  file2  file3
```
### Command-line client

### Backup scheduler

The backup scheduler allows you to perform snapshots...

## How it works

## Terminology

### Backup

A **backup** represents the state of the file/directory at a given time, it also holds a reference to a **meta**. attached to it.

The hash of a backup is: ``SHA1(hostname + path + timestamp)``.

### Snapshots

Multiple **backups** of the same file/directory form a **snapshot**. If you backup a directory only once, it will create a **snapshot** with 1 **backup** and so on.

The hash of a snapshots groups is ``SHA1(hostname + path)``.

### Blobs

A **blob** (binary large object) is where chunks are stored. **Blobs** are immutable and stored with the SHA-1 hash as filename.

**Blobs** are stored in a **backend**.

### Metas

A **meta** (stored as a hash) holds the file/directory metadata, like filename, size, type (file/directory) and a reference to the directory content (a set) or the file chunks (a sorted list).

Multiple **backups** may refers to the same **meta** if the content is the same.

The Hash of a meta is: ``SHA1(filename + file hash)``.

### Backend

A **backend** handle blobs operation (blobsfile/s3/encrypt/mirror/remote).

- Put
- Exists
- Get
- Enumerate

You can combine backend as you wish, e.g. Mirror( Encrypt( S3() ), BlobsFile() ).

## Under the hood

### Talks to the DB with using Redis protocol

You can inspect the database with any Redis-compatible client:

```console
$ redis-cli -p 9736
127.0.0.1:9736> ping
PONG
127.0.0.1:9736> 
```

### Monitor commands

You can monitor the database using the Server-Sent Events API:

```console
$ curl http://0.0.0.0:9737/debug/monitor
data: hgetall command  with args [2c07e10cd475290b49f5f943315b65f6a16192b5] from client: 127.0.0.1:42179

data: hgetall command  with args [92d430a8dbe6a6ace8db423d231ead6a4bf33634] from client: 127.0.0.1:42179

```

### Debug variables

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

### Metadata format

Metadata are stored in in a kv file and are exposed via a Redis protocol tcp server, with custom Redis-like data type and commands, but implemented using kv lexicographical range queries.

- String data type
- Hash data type
- Set (lexicographical order) data type (used to store hash list)
- List (sorted by an uint index) data type
- "Virtual" Blob data type (upload/download from/to storage)

Check [db/buffer.go](https://github.com/tsileo/datadatabase/blob/master/server/buffer.go) for more documentation.

A backup is a set with pointer to hash (either representing a directory or a file, and a directory is also a set of pointer).

If a file is stored multiple times, metadata are not duplicated.

A hash contains the backup parts reference, an ordered list of the files hash blobs.

## Roadmap / Ideas

- Follow .gitignore file
- A special cold storage backed (using AWS Glacier, can't use glacier since storing blobs with Glacier would cost too much, according to [this article](http://alestic.com/2012/12/s3-glacier-costs)) that would put one archive per snapshots, and keep track of stored blob (incremental backups).
- Garbage collection (sparse files support for blob files)
- A web interface
- Fill an issue!
