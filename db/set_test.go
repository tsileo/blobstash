package db

import (
	"bytes"
	"testing"
)

func TestDBSetDataType(t *testing.T) {
	db, err := NewMem()
	if err != nil {
		panic("db error")
	}
	defer func() {
		db.Close()
		db.Destroy()
	}()

	card, err := db.Scard("foo")
	check(err)
	if card != 0 {
		t.Error("Inexistent set should have a cardinality of 0")
	}

	cnt := db.Sadd("foo", "a", "a", "b", "b")
	if cnt != 2 {
		t.Errorf("only 2 elements should have been inserted, got %v", cnt)
	}

	cnt = db.Sadd("foo", "b", "c")
	if cnt != 1 {
		t.Errorf("only 1 elements should have been inserted, got %v", cnt)
	}

	cnt = db.Sismember("foo", "c")
	if cnt != 1 {
		t.Error("c should be part of set foo")
	}

	cnt = db.Sismember("foo", "d")
	if cnt > 0 {
		t.Error("d shouldn't be part of the set")
	}

	members := db.Smembers("foo")
	if len(members) != 3 {
		t.Errorf("foo members should have a len of 3, got %v", len(members))
	}

	expected := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	for i, member := range members {
		if !bytes.Equal(member, expected[i]) {
			t.Errorf("Bad set member, expected %v, got %v", expected[i], member)
		}
	}

	card, err = db.Scard("foo")
	check(err)
	if card != 3 {
		t.Error("foo set should have a cardinality of 3")
	}

	err = db.Sdel("foo")
	check(err)

	card, err = db.Scard("foo")
	check(err)
	if card != 0 {
		t.Error("Inexistent set should have a cardinality of 0")
	}

}
