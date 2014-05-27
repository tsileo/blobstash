package s3

import (
	"testing"

	"github.com/tsileo/datadatabase/backend"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func TestS3Backend(t *testing.T) {
	b := New("thomassileotestdatadb6161", "eu-west-1")
	defer func() {
		if err := b.Drop(); err != nil {
			panic(err)
		}
	}()
	backend.Test(t, b)
}
