package db

import (
	"reflect"
	"testing"
)

func TestDBStringDataType(t *testing.T) {
	db, err := New("db_string")
	defer db.Destroy()
	if err != nil {
		panic("db error")
	}
	defer func() {
		db.Close()
		db.Destroy()
	}()

	err = db.Put("foo", "bar")
	check(err)
	err = db.Put("foo2", "bar2")
	check(err)
	err = db.Put("foo3", "bar3")
	check(err)


	kvs, err := db.GetStringRange("", "\xff", 10)
	check(err)

	if len(kvs) != 3 {
		t.Errorf("Range should be 3, got %v", len(kvs))
	}
	expected := []*KeyValue{&KeyValue{"foo", "bar"}, &KeyValue{"foo2", "bar2"},
		&KeyValue{"foo3", "bar3"}}
	if !reflect.DeepEqual(expected, kvs) {
		t.Errorf("Range error, expected:%+v, got: %+v", expected, kvs)
	}

	db.Del("foo")
	db.Del("foo2")
	db.Del("foo3")
}
