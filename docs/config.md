# Config

## Example

### BlobsFile and S3 with two namespaces

When using S3, ``S3_SECRET_KEY`` and ``S3_ACCESS_KEY`` environment variables must be set.

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
