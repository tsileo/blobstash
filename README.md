Data Database
=============

## Overview

A backup database built on top of [kv](https://github.com/cznic/kv) and the [Redis Protocol](http://redis.io/topics/protocol), bundled with a command-line client and a FUSE file system.

Draws inspiration from [Camlistore](camlistore.org) and [bup](https://github.com/bup/bup) (files are split into multiple blobs using a rolling checksum).

## Features:
 
- Snapshots
- Content addressed, files are split into blobs, and retrieved by hash
- Incremental backups/data deduplication
- Server handles uploading/downloading blobs to/from different storage
- Client only query the server and send blobs to it (the client take care of chunking/building blobs).
- Read-only FUSE file system to navigate backups/snapshots

### Snapshots

If you backup a directory/file with the same filename more than once, it will be grouped as a snapshot.

### Blobs

Blobs are handled by the server, you only perform three operations on blobs:

- (CMD arg => reply)
- BPUT content => hash
- BGET hash => content
- BEXISTS hash => bool

Blobs are store as file (or key/archive) with its sha1 as filename in a flat directory (or bucket/vault).

### Metadata

Metadata are stored in kv database and are exposed via a Redis protocol tcp server, with custom Redis-like data type and commands, but implemented using kv lexicographical range queries.

- String data type
- Hash data type
- Set (lexicographical order) data type (used to store hash list)
- List (sorted by an uint index) data type
- "Virtual" Blob data type (upload/download from/to storage)

A backup is a set with pointer to hash (either representing a directory or a file, and a directory is also a set of pointer).

If a file is stored multiple times, metadata are not duplicated.

A hash contains the backup parts reference, an ordered list of the files hash blobs.

### Databases

Databases are actually different kv databases, so you can export/import the meta data to be backup along with blobs.

A database is tied to a storage.

## Roadmap / Ideas

- Add a **drop** directory to support upload via FUSE
- Easy way to backup/restore internal kv (RDB like format)
- Master/slave replication of metadatas
- Encryption
- Periodic snapshot/snapshots monitoring
- A special cold storage backed (using AWS Glacier, can't use glacier since storing blobs with Glacier would cost too much, according to [this article](http://alestic.com/2012/12/s3-glacier-costs)) that would put one archive per snapshots, and keep track of stored blob (incremental backups).

## Supported storages

- Local/SFTP
- S3 (not started yet)
- Submit a pull request!
