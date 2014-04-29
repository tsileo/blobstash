package db

import (
	"testing"
	"reflect"
)

func TestDBStringDataType(t *testing.T) {
	db, err := NewMem()
	if err != nil {
		panic("db error")
	}
	defer func() {
		db.Close()
		db.Destroy()
	}()

	cnt, err := db.GetStringCnt()
	check(err)
	if cnt != 0 {
		t.Errorf("String cnt should be 0, not %v", cnt)
	}

	err = db.Put("foo", "bar")
	check(err)
	err = db.Put("foo2", "bar2")
	check(err)
	err = db.Put("foo3", "bar3")
	check(err)

	cnt, err = db.GetStringCnt()
	check(err)
	if cnt != 3 {
		t.Errorf("String cnt should be 3, not %v", cnt)
	}

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

	cnt, err = db.GetStringCnt()
	check(err)
	if cnt != 0 {
		t.Errorf("String cnt should be 0, not %v", cnt)
	}
}
