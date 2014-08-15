# Config

## Example

### BlobsFile and S3 with two namespaces

When using [S3](http://aws.amazon.com/s3/), ``S3_SECRET_KEY`` and ``S3_ACCESS_KEY`` environment variables must be set.

```json
{
    "backends": {
        "blobs": {
            "backend-type": "blobsfile",
            "backend-args": {
                "path": "blobs",
                "compression": true
            }
        },
        "s3": {"backend-type": "s3",
            "backend-args": {
                "bucket": "bucketname",
                "location": "eu-west-1"
            }
        }
    },
    "router": [
    	["if-ns-s3", "s3"],
        ["default", "blobs"]
    ]
}
```

### Encrypt to S3 with one namespace

The **encrypt** backup need a encryption key (used by [go.crypto/nacl secretbox](http://godoc.org/code.google.com/p/go.crypto/nacl)), defined in **key-path**, it must contains a 32 bytes (256bits) key.

```json
{
    "backends": {
        "blobs": {
            "backend-type": "encrypt",
            "backend-args": {
                "key-path": "/home/thomas/blobstash.key",
                "dest": {
                    "backend-type": "s3",
                    "backend-args": {
                        "bucket": "bucketname",
                        "location": "eu-west-1"
                    }
                }
            }
        },
        "router": [
            [
                "default",
                "blobs"
            ]
        ]
    }
}
```
