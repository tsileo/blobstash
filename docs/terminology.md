# Terminology

## Backup

A **backup** represents the state of the file/directory at a given time, it also holds a reference to a **meta**. attached to it.

The hash of a backup is: ``SHA1(hostname + path + timestamp)``.

## Snapshots

Multiple **backups** of the same file/directory form a **snapshot**. If you backup a directory only once, it will create a **snapshot** with 1 **backup** and so on.

The hash of a snapshots groups is ``SHA1(hostname + path)``.

## Blobs

A **blob** (binary large object) is where chunks are stored. **Blobs** are immutable and stored with the SHA-1 hash as filename.

**Blobs** are stored in a **backend**.

## Metas

A **meta** (stored as a hash) holds the file/directory metadata, like filename, size, type (file/directory) and a reference to the directory content (a set) or the file chunks (a sorted list).

Multiple **backups** may refers to the same **meta** if the content is the same.

The Hash of a meta is: ``SHA1(filename + type (dir or file) + size + mode + mtime + ref)``.
