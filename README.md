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

### Databases

Databases are actually different LevelDB databases, so you can export/import the meta data to be backup along with blobs.

## Roadmap / Ideas

- Mount backups with fuse
- Easy way to backup/restore internal LevelDB
- Master/slave replication of metadatas
- Optional Elastic search powered searches

## Supported storages

- Local
- S3 (not started yet)
- Glacier (not started yet)
