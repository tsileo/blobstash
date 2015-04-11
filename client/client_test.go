package client

import "testing"

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func TestMetaEncodeDecode(t *testing.T) {
	kvs := NewKvStore("")
	_, err := kvs.Put("k1", "v1", -1)
	check(err)
}
