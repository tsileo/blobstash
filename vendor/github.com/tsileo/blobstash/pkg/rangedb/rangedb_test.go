package rangedb

import (
	"bytes"
	"fmt"
	"io"
	"reflect"
	"testing"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func getRange(t *testing.T, db *RangeDB, start, end []byte, r bool) [][]byte {
	var out [][]byte
	c := db.Range(start, end, r)

	k, v, err := c.Next()
	t.Logf("err after next=%+v %+v %+v", err, k, v)
	for ; err == nil; k, v, err = c.Next() {
		out = append(out, k)
		t.Logf("k=%s, v=%s", k, v)
	}
	if err == io.EOF {
		t.Logf("EOF")
		return out
	}
	check(err)
	return out
}

func TestDBBasic(t *testing.T) {
	db, err := New("db_base")
	defer db.Destroy()
	if err != nil {
		t.Fatalf("Error creating db %v", err)
	}
	check(db.Set([]byte("aello01"), []byte("lol")))
	out := [][]byte{}
	for i := 0; i < 100; i++ {
		k := []byte(fmt.Sprintf("hello%03d", i))
		check(db.Set(k, []byte("lol")))
		out = append(out, k)
	}
	check(db.Set([]byte("zello01"), []byte("lolzello")))

	val, err := db.Get([]byte("zello01"))
	check(err)
	if !bytes.Equal(val, []byte("lolzello")) {
		t.Errorf("get failed")
	}

	r1 := getRange(t, db, []byte("hello010"), []byte("hello030"), false)
	if !reflect.DeepEqual(r1, out[10:31]) {
		t.Errorf("range check failed")
	}

	r2 := getRange(t, db, []byte("hello"), []byte("hello\xff"), false)
	if !reflect.DeepEqual(r2, out) {
		t.Errorf("range check failed")
	}

	// Reverse the original data
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}

	t.Logf("before r3")
	r3 := getRange(t, db, []byte("hello080"), []byte("hello\xff"), true)
	t.Logf("len r3=%d", len(r3))
	if !reflect.DeepEqual(r3, out[0:20]) {
		t.Errorf("range check failed")
	}

	r4 := getRange(t, db, []byte("hello"), []byte("hello\xff"), true)
	if !reflect.DeepEqual(r4, out) {
		t.Errorf("range check failed")
	}
}
