# Build app using BlobDB

You can use BlobDB as a Redis-like data structure server, bundled with a blob store.

If you want to have a look at an existing application using BlobDB, you can check out [BlobPad](https://github.com/tsileo/blobpad).

## Design pattern

Data stored in BlobDB is immutable, but you can use sorted list when you need to handle a mutable attribute, like a title.

	> LADD attr-key current-timestamp attr-value
