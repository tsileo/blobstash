package meta

import (
	"reflect"
	"testing"

	"github.com/tsileo/blobstash/vkv"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func TestMetaEncodeDecode(t *testing.T) {
	kv := &vkv.KeyValue{
		Key:     "foo1",
		Value:   "foo2",
		Version: 1421580005947392157,
	}
	data := encodeKv(kv)
	kv2 := decodeKv(data)
	if !reflect.DeepEqual(kv, kv2) {
		t.Errorf("encoding/decoding failed, encoded:%+v, decoded:%+v", kv, kv2)
	}
	h := "c0f1480a26c2fd4deb8e738a52b7530ed111b9bcd17bbb09259ce03f129988c5"
	ns := "nstest"
	blob := CreateNsBlob(h, ns)
	h2, ns2 := DecodeNsBlob(blob)
	if h != h2 || ns != ns2 {
		t.Errorf("failed to encode/decode NsBlob: got hash %v, expected %v, got ns %v, expected %v", h, h2, ns, ns2)
	}
}
