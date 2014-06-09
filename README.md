Data Database
=============

## Overview

A backup database (a Content-Addressable Storage and a data structure server) designed to efficiently handle snapshots of files/directories, built on top of [kv](https://github.com/cznic/kv) and the [Redis Protocol](http://redis.io/topics/protocol), bundled with a command-line client and a FUSE file system.

Draws inspiration from [Camlistore](camlistore.org) and [bup](https://github.com/bup/bup) (files are split into multiple blobs using a rolling checksum).

## Features:

- Content addressed, files are split into blobs, and retrieved by hash
- Incremental backups/snapshots thanks to data deduplication
- Server handles uploading/downloading blobs to/from different storage
- Client only query the server and send blobs to it (the client take care of chunking/building blobs).
- Read-only FUSE file system to navigate backups/snapshots
- Encryption using [go.crypto/nacl secretbox](http://godoc.org/code.google.com/p/go.crypto/nacl)
- Take snapshot automatically every x minutes, using a separate client-side daemon (provides Arq/time machine like backup)

## How it works

First, a transaction is initiated (if something goes wrong, no stale data will be saved and the transaction will be discarded).

If the source is a directory, it will be uploaded recursively, if it's a file, it will be split into multiple blobs/chunks (blobs will be sent only if it doesn't exists yet), while performing the backup, meta data (size, name, hashes, directory trees) are sent to the server and buffered until the backup is done.

If something has gone wrong, the transaction is discarded. If everything is OK, the transaction is applied and a meta blob will be created.

## Terminology

### Backup

A **backup** represents the state of the file/directory at a given time, it also holds a reference to a **meta**. attached to it.

The hash of a backup is: ``SHA1(hostname + path + timestamp)``.

### Snapshots

Multiple **backups** of the same file/directory form a **snapshot**. If you backup a directory only once, it will create a **snapshot** with 1 **backup** and so on.

### Blobs

A **blob** (binary large object) is where chunks are stored. **Blobs** are immutable and stored with the SHA-1 hash as filename.

**Blobs** are stored in a **database**.

### Metas

A **meta** (stored as a hash) holds the file/directory metadata, like filename, size, type (file/directory) and a reference to the directory content (a set) or the file chunks (a sorted list).

Multiple **backups** may refers to the same **meta** if the content is the same.

The Hash of a meta is: ``SHA1(filename + file hash)``.

### Databases

**Databases** are actually different kv databases (hold the index), so you can export/import the **meta** data to be backup along with **blobs**.

A **database** is tied to a **backend**.

### Backend

A **backend** handle blobs operation (blobsfile/s3/encrypt/mirror/remote).

- Put
- Exists
- Get
- Enumerate

You can combine backend as you wish, e.g. Mirror( Encrypt( S3() ), BlobsFile() ).

## Getting Started

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

### Metadata format

Metadata are stored in kv database and are exposed via a Redis protocol tcp server, with custom Redis-like data type and commands, but implemented using kv lexicographical range queries.

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
- Garbage collection

## Supported storages

- Local
- S3
- A remote DataDB instance
- Mirror (not started yet)
- Glacier (not started yet)
- Submit a pull request!
