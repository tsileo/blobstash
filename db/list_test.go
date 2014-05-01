package db

import (
	"testing"
	"bytes"
	"reflect"
)

func TestDBListDataType(t *testing.T) {
	db, err := NewMem()
	if err != nil {
		panic("db error")
	}
	defer func() {
		db.Close()
		db.Destroy()
	}()

	cnt, err := db.Llen("foo")
	check(err)
	if cnt != 0 {
		t.Error("Inexistent list should have a len of 0")
	}

	err = db.Ladd("foo", 0, "0")
	check(err)
	err = db.Ladd("foo", 1, "1")
	check(err)
	err = db.Ladd("foo", 10, "10")
	check(err)

	cnt, err = db.Llen("foo")
	check(err)
	if cnt != 3 {
		t.Error("List should have a len of 3")
	}

	data, err := db.Lindex("foo", 10)
	check(err)
	if !bytes.Equal(data, []byte("10")) {
		t.Errorf("List value should be 10, got %v", string(data))
	}

	rdata, err := db.Liter("foo")
	check(err)
	if !reflect.DeepEqual(rdata, [][]byte{[]byte("0"), []byte("1"), []byte("10")}) {
		t.Error("Bad LITER result")
	}


	err = db.Ladd("foo", 20, "20")
	check(err)

	ivs, err := db.GetListRangeWithPrevAndIndex("foo", 15, 20, 0)
	check(err)
	if len(ivs) != 2 {
		t.Errorf("GetListRangeWithPrevAndIndex expected len:%v, got:%v", 2, len(ivs))
	}
	if ivs[0].Index != 10 {
		t.Errorf("Bad IndexValue, expected 10, got %+v", ivs[0])
	}

	err = db.Ldel("foo")
	check(err)

	cnt, err = db.Llen("foo")
	check(err)
	if cnt != 0 {
		t.Error("Inexistent list should have a len of 0")
	}
}
