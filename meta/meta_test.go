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
}
