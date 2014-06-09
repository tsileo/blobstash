package blobsfile

import (
	"os"
	"testing"
	"fmt"
	"crypto/sha1"
)

func TestBlobsIndex(t *testing.T) {
	index, err := NewIndex("tmp_test_index")
	check(err)
	defer index.Close()
	defer os.RemoveAll("tmp_test_index")

	bp := &BlobPos{n: 1, offset: 5, size: 10}
	h := fmt.Sprintf("%x", sha1.New().Sum([]byte("fakehash")))
	err = index.SetPos(h, bp)
	check(err)
	bp3, err := index.GetPos(h)
	if bp.n != bp3.n || bp.offset != bp3.offset || bp.size != bp3.size {
		t.Errorf("index.GetPos error, expected:%q, got:%q", bp, bp3)
	}

	err = index.SetN(5)
	check(err)
	n2, err := index.GetN()
	check(err)
	if n2 != 5 {
		t.Errorf("Error GetN, got %v, expected 5", n2)
	}
	err = index.SetN(100)
	check(err)
	n2, err = index.GetN()
	check(err)
	if n2 != 100 {
		t.Errorf("Error GetN, got %v, expected 100", n2)
	}
}
