# Troubleshooting

## Router not working

The default rules should be the last.

Bad:

	"router": [
	    ["default", "blobs"],
        ["if-ns-blobpad", "s3"]
    ]

Every request will be router to default (blobs).

Good:

	"router": [
        ["if-ns-blobpad", "s3"],
        ["default", "blobs"]
    ]
