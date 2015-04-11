# Under the hood

## Debug variables

You can check debug vars via the [expvar](http://golang.org/pkg/expvar/) interface:

```console
$ curl http://0.0.0.0:9737/debug/vars
{
  "blobsfile-blobs-downloaded": {}, 
  "blobsfile-blobs-uploaded": {
    "/box/blobsfileconfigcreatedmeta2": 1
  }, 
  "blobsfile-bytes-downloaded": {}, 
  "blobsfile-bytes-uploaded": {
    "/box/blobsfileconfigcreatedmeta2": 472
  }, 
  "blobsfile-open-fds": {
    "/box/blobsfileconfigcreatedblobs2": 2, 
    "/box/blobsfileconfigcreatedmeta2": 2
  }, 
  "cmdline": [
    "/tmp/go-build248426472/command-line-arguments/_obj/exe/server"
  ], 
  "encrypt-blobs-downloaded": {}, 
  "encrypt-blobs-uploaded": {}, 
  "encrypt-bytes-downloaded": {}, 
  "encrypt-bytes-uploaded": {}, 
  "glacier-blobs-downloaded": {}, 
  "glacier-blobs-uploaded": {}, 
  "glacier-bytes-downloaded": {}, 
  "glacier-bytes-uploaded": {}, 
  "memstats": {
    [...]
  }, 
  "mirror-blobs-downloaded": {}, 
  "mirror-blobs-uploaded": {}, 
  "mirror-bytes-downloaded": {}, 
  "mirror-bytes-uploaded": {}, 
  "server-started-at": "12 Jun 14 20:11 +0000", 
  "server-total-connections-received": 22
}
```

## Metadata format

