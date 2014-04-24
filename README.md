Data Database
=============

## Overview

Draws inspiration from [Camlistore](camlistore.org) and [bup](https://github.com/bup/bup) (files are split into multiple blobs using a rolling checksum).

A backup database built on top of [LevelDB](https://code.google.com/p/leveldb/), and [elasticsearch](http://www.elasticsearch.org/) and the [Redis Protocol](http://redis.io/topics/protocol).

## Features:
 
- Content addressed, files are split into blobs, and retrieved by hash
- Incremental backups by default

### Blobs

Blobs are store as file with its sha1 as filename in a flat directory.

### Metadata

Metadata are stored in LevelDB and are exposed via a Redis protocol tcp server, with custom Redis-like data type and commands, but implemented using LevelDB lexicographical range queries and snapshots.

- Redis-like transactions
- Snapshot handling
- String
- Hashmap
- Set
- Backup part (custom zset)

A backup is a set with pointer to hashmap (either representing a directory or a file, and a directory is also a set of pointer).

Hashmap pointer are the SHA1 of the JSON object, so if a file is stored multiple times, metadata are not duplicated.

A hashmap contains the backup parts reference, an ordered list of the files hash blobs.

### Databases

Databases are actually different LevelDB databases, so you can export/import the meta data to be backup along with blobs.

A database is tied to a storage.

## Roadmap / Ideas

- Mount backups with fuse
- Easy way to backup/restore internal LevelDB
- Master/slave replication of metadatas
- Optional Elastic search powered searches

## Supported storages

- Local
- S3 (not started yet)
- Glacier (not started yet)
