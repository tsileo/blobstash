# BlobsFile

[![builds.sr.ht status](https://builds.sr.ht/~tsileo/blobsfile.svg)](https://builds.sr.ht/~tsileo/blobsfile?)
&nbsp; &nbsp;[![Godoc Reference](https://godoc.org/a4.io/blobsfile?status.svg)](https://godoc.org/a4.io/blobsfile)

*BlobsFile* is an append-only (i.e. no update and no delete) content-addressed *blob store* (using [BLAKE2b](https://blake2.net/) as hash function).

It draws inspiration from Facebook's [Haystack](http://202.118.11.61/papers/case%20studies/facebook.pdf), blobs are stored in flat files (called _BlobFile_) and indexed by a small [kv](https://github.com/cznic/kv) database for fast lookup.

*BlobsFile* is [BlobStash](https://github.com/tsileo/blobstash)'s storage engine.

## Features

 - Durable (data is fsynced before returning)
 - Immutable (append-only, can't mutate or delete blobs)
 - Optional compression (Snappy or Zstandard)
 - Extra parity data is added to each _BlobFile_ (using Reed-Solomon error correcting code), allowing the database to repair itself in case of corruption.
   - The test suite is literraly punching holes at random places
