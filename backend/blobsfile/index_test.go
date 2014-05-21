package blobsfile

import (
	"testing"
	"os"
)

func TestBlobsIndex(t *testing.T) {
	index, err := NewIndex("tmp_test_index")
	check(err)
	defer index.Close()
	defer os.RemoveAll("tmp_test_index")

	bp := &BlobPos{n:1, offset:5, size:10}
	bpString := bp.String()
	if bpString != "1 5 10" {
		t.Errorf("Bad BlobPos serialization, expected:%q, got:%q", "1 5 10", bpString)
	}
	bp2, err := ScanBlobPos(bpString)
	check(err)
	if bp.n != bp2.n || bp.offset != bp2.offset || bp.size != bp2.size {
		t.Errorf("BlobPos scan error, expected:%q, got:%q", bp, bp2)
	}
	err = index.SetPos("fakehash", bp2)
	check(err)
	bp3, err := index.GetPos("fakehash")
	if bp.n != bp3.n || bp.offset != bp3.offset || bp.size != bp3.size {
		t.Errorf("index.GetPos error, expected:%q, got:%q", bp, bp3)
	}
}
