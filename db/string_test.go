package db

import (
	"testing"
)

func TestDBStringDataType(t *testing.T) {
	db, err := New("test_db_string")
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
	err = db.Put("foo2", "bar")
	check(err)
	err = db.Put("foo3", "bar")
	check(err)

	cnt, err = db.GetStringCnt()
	check(err)
	if cnt != 3 {
		t.Errorf("String cnt should be 3, not %v", cnt)
	}

	_, snapId := db.CreateSnapshot()

	err = db.Put("foo4", "bar")
	check(err)
	
	kvs, err := db.GetStringRange(snapId, "", "\xff", 10)
	check(err)

	if len(kvs) != 3 {
		t.Errorf("Range should be 3, got %v", len(kvs))
	}
	if kvs[0].Key != "foo" {
		t.Errorf("First key should be foo, got %v", kvs[0].Key)
	}

	db.ReleaseSnapshot(snapId)

	cnt, err = db.GetStringCnt()
	check(err)
	if cnt != 4 {
		t.Errorf("String cnt should be 4, not %v", cnt)
	}

	db.Del("foo")
	db.Del("foo2")
	db.Del("foo3")
	db.Del("foo4")

	cnt, err = db.GetStringCnt()
	check(err)
	if cnt != 0 {
		t.Errorf("String cnt should be 0, not %v", cnt)
	}
}
