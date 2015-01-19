# Blob Store

You can deal directly with blobs when needed using the HTTP API.

The default address is **http://localhost:8050**.

## Endpoints

### POST /api/v1/blobstore/upload

You can upload blobs by using a standard multi-part form.

If the form name doesn't match the [BLAKE2b](https://blake2.net) hash of the content, the blob will be discarded.

```console
$ curl -F "c0f1480a26c2fd4deb8e738a52b7530ed111b9bcd17bbb09259ce03f129988c5=ok" http://0.0.0.0:8050/api/v1/blobstore/upload
```

### HEAD /api/v1/blobstore/blob/<hash>

Returns status code 200 if the blob exists, 404 if it doesn't exist.

```console
$ curl -I  http://0.0.0.0:8050/api/v1/blobstore/blob/c0f1480a26c2fd4deb8e738a52b7530ed111b9bcd17bbb09259ce03f129988c7
HTTP/1.1 404 Not Found
Content-Type: text/plain; charset=utf-8
Date: Wed, 30 Jul 2014 11:09:25 GMT
Content-Length: 10
```

### GET /api/v1/blobstore/blob/<hash>

Returns the blob content.

```console
$ curl http://0.0.0.0:8050/api/v1/blobstore/blob/c0f1480a26c2fd4deb8e738a52b7530ed111b9bcd17bbb09259ce03f129988c5
ok
```
