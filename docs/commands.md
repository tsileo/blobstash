# Commands

BlobStash talks Redis protocol called [RESP](http://redis.io/topics/protocol) (REdis Serialization Protocol).

Check the [client](http://godoc.org/github.com/tsileo/blobstash/client) and the source for now.

## Strings

### GET key

Get the value of key, returns nil if unset.

**Return value**

bulk strings: the value of key, or nil when key does not exist.

## Hashes

## Indexed lists

