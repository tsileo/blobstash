package s3

import (
	"os"
	"testing"

	"github.com/tsileo/blobstash/backend"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func TestS3Backend(t *testing.T) {
	if os.Getenv("S3_TEST_BUCKET") == "" || os.Getenv("S3_ACCESS_KEY") == "" || os.Getenv("S3_SECRET_KEY") == "" {
		t.Skip("Skipping TestS3Backend, environment variable S3_TEST_BUCKET/S3_ACCESS_KEY/S3_SECRET_KEY not set.")
	}
	b := New(os.Getenv("S3_TEST_BUCKET"), "eu-west-1")
	defer func() {
		if err := b.Drop(); err != nil {
			panic(err)
		}
	}()
	backend.Test(t, b)
}
