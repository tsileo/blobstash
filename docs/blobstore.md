# Blob Store

You can deal directly with blobs when needed using the HTTP API, full docs [here](docs/blobstore.md).

The default address is **http://localhost:9736**.

## Headers

All the API endpoints makes use of these two HTTP headers:

- **BlobStash-Namespace**: used to define the blob namespace
- **BlobStash-Meta**: must be 0/1/yes/no/true/false, if true, the blob will be applied as meta blobs, false by default.

## Endpoints

### POST /upload

You can upload blobs by using a standard multi-part form.

If the form name doesn't match the [BLAKE2b](https://blake2.net) hash of the content, the blob will be discarded.

```console
$ curl -H "BlobStash-Namespace: ok2" -F "c0f1480a26c2fd4deb8e738a52b7530ed111b9bcd17bbb09259ce03f129988c5=ok" http://0.0.0.0:9736/upload
```

And if you need to apply a meta blob, just add the **BlobStash-Meta** HTTP header:

```console

$ curl -H "BlobStash-Namespace: ok2" -H "Blobstash-Meta: 1" -F "c0f1480a26c2fd4deb8e738a52b7530ed111b9bcd17bbb09259ce03f129988c5=ok" http://0.0.0.0:9736/upload
```

### HEAD /blob/<hash>

Returns status code 200 if the blob exists, 404 if it doesn't exist.

```console
$ curl -I -H "BlobStash-Namespace: ok2" http://0.0.0.0:9736/blob/c0f1480a26c2fd4deb8e738a52b7530ed111b9bcd17bbb09259ce03f129988c7
HTTP/1.1 404 Not Found
Content-Type: text/plain; charset=utf-8
Date: Wed, 30 Jul 2014 11:09:25 GMT
Content-Length: 10
```

### GET /blob/<hash>

Returns the blob content.

```console
$ curl -H "BlobStash-Namespace: ok2" http://0.0.0.0:9736/blob/c0f1480a26c2fd4deb8e738a52b7530ed111b9bcd17bbb09259ce03f129988c5
ok
```
