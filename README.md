Data Database
=============

## Overview

Draws inspiration from [Camlistore](camlistore.org) and [bup](https://github.com/bup/bup) (files are split into multiple blobs using a rolling checksum).

A backup database built on top of [LevelDB](https://code.google.com/p/leveldb/), and [elasticsearch](http://www.elasticsearch.org/) and the [Redis Protocol](http://redis.io/topics/protocol).

Features:
- 
- Content addressed, files are split into blobs, and retrieved by hash
- Incremental backups by default

Supported storages

- Local
- S3
- Glacier
