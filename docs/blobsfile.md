# BlobsFile

**BlobsFile** is the default blob backend, mutliple blobs are stored (optionally compressed with Snappy) inside "BlobsFile"/fat file/packed file (256MB by default, every new file are "fallocate"d).

Blobs are indexed by a small [kv](https://github.com/cznic/kv) database.

New blobs are appended to the current file, and when the file exceed the limit, a new fie is created.

Blobs are stored with its hash and its size (for a total overhead of 36 bytes by blobs) followed by the blob itself, thus allowing re-indexing.

## Write-only mode

The backend also support a write-only mode (Get operation is disabled in this mode)
where older blobsfile aren't needed since they won't be loaded (used for cold storage,
once uploaded, a blobsfile can be removed but are kept in the index, and since no metadata
is needed for blobs, this format may be better than a tar archive).
