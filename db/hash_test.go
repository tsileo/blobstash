package db

import (
	"testing"
	"bytes"
	"reflect"
)

func TestDBHashDataType(t *testing.T) {
	db, err := NewMem()
	if err != nil {
		panic("db error")
	}
	defer func() {
		db.Close()
		db.Destroy()
	}()

	hlen, err := db.Hlen("foo")
	check(err)
	if hlen != 0 {
		t.Error("Inexistent hash should have a length of 0")
	}

	cnt, err := db.Hset("foo", "attr1", "val1")
	check(err)
	if cnt != 1 {
		t.Error("Bad hset result")
	}

	res, err := db.Hget("foo", "attr1")
	check(err)
	if !bytes.Equal(res, []byte("val1")) {
		t.Error("bad hget result")
	}

	cnt, err = db.Hexists("foo", "attr2")
	check(err)
	if cnt != 0 {
		t.Error("Attribute shouldn't exist")
	}
	
	cnt, err = db.Hexists("foo", "attr1")
	check(err)
	if cnt != 1 {
		t.Error("Attribute should exist")
	}

	cnt, err = db.Hmset("foo", "attr1", "val1", "attr2", "val2", "attr3", "val3")
	check(err)
	if cnt != 2 {
		t.Error("Only 2 attributes should have been created")
	}

	expected := []*KeyValue{&KeyValue{"attr1", "val1"}, &KeyValue{"attr2", "val2"},
							&KeyValue{"attr3", "val3"}}
	kvs, err := db.Hgetall("foo")
	check(err)
	if !reflect.DeepEqual(kvs, expected) {
		t.Errorf("Bad hgetall result, got: %+v, expected: %+v", kvs, expected)
	}

	
	hlen, err = db.Hlen("foo")
	check(err)
	if hlen != 3 {
		t.Errorf("Hash should have 3 fields, got %v", hlen)
	}

}
