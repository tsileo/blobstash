# BlobsFile

[![Build Status](https://travis-ci.org/tsileo/blobsfile.svg?branch=master)](https://travis-ci.org/tsileo/blobsfile)
&nbsp; &nbsp;[![Godoc Reference](https://godoc.org/a4.io/blobsfile?status.svg)](https://godoc.org/a4.io/blobsfile)
&nbsp; &nbsp;[![Go Report Card](https://goreportcard.com/badge/a4.io/blobsfile)](https://goreportcard.com/report/a4.io/blobsfile)

*BlobsFile* is an append-only (i.e. no update and no delete) content-addressed *blob store* (using [BLAKE2b](https://blake2.net/) as hash function).

It draws inspiration from Facebook's [Haystack](http://202.118.11.61/papers/case%20studies/facebook.pdf), blobs are stored in a flat file and indexed in a small [kv](https://github.com/cznic/kv) database.

*BlobsFile* is [BlobStash](https://github.com/tsileo/blobstash)'s storage engine.

## Features

 - Durable (data is fsynced before returning)
 - Immutable (append-only, can't mutate or delete blobs)
 - Extra parity data is added to each BlobFile (using Reed-Solomon error correcting code), allowing the database to repair itself in case of corruption.

