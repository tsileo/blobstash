# Terminology

## Snapshot

A **snapshot** is the state of the directory/file at a given time, it holds a reference to a **meta**.

The hash of a snapshot is: ``SHA-1(hostname + absolute path + unix timestamp)``.

## Backup

A **backup** is a set of snapshot that share the same hostname and absolute path.

## Blobs

Files are split into multiple chunks and are stored as **blob** (binary large object) in **backend**.

**Blobs** are immutable and identified with the SHA-1 hash of the chunk.

## Backend

## Metas

A **meta** (stored as a hash) holds the file/directory metadata, like filename, size, type (file/directory) and a reference to the directory content (a set) or the file chunks (a sorted list).

Multiple **backups** may refers to the same **meta** if the content is the same.

The Hash of a meta is: ``SHA1(filename + type (dir or file) + size + mode + mtime + ref)``.
