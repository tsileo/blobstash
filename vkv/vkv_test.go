package vkv

import (
	"reflect"
	"sync"
	"testing"
	"time"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}
func TestDB(t *testing.T) {
	db, err := New("db_base")
	defer db.Destroy()
	if err != nil {
		t.Fatalf("Error creating db %v", err)
	}
	// testing low level binary encoded uint64
	valUint, err := db.getUint64([]byte("foo2"))
	check(err)
	if valUint != uint64(0) {
		t.Errorf("Uninitialized uint64 key should return 0")
	}
	err = db.putUint64([]byte("foo2"), uint64(5))
	check(err)
	valUint, err = db.getUint64([]byte("foo2"))
	check(err)
	if valUint != uint64(5) {
		t.Error("Key should be set to 5")
	}
	err = db.incrUint64([]byte("foo2"), 2)
	check(err)
	valUint, err = db.getUint64([]byte("foo2"))
	check(err)
	if valUint != uint64(7) {
		t.Error("Key should be set to 7")
	}
	var wg sync.WaitGroup
	// Test the mutex
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err = db.incrUint64([]byte("foo3"), 1)
			check(err)
		}()
	}
	wg.Wait()
	valUint, err = db.getUint64([]byte("foo3"))
	check(err)
	if valUint != uint64(10) {
		t.Errorf("Key foo3 should be set to 10, got %v", valUint)
	}
	rnotfound, err := db.Get("test_key_1", -1)
	if err != ErrNotFound {
		t.Errorf("key %v shouldn't exists got %v/%v", rnotfound, err)
	}

	keys, err := db.Keys("", "\xff", 0)
	check(err)
	if len(keys) != 0 {
		t.Errorf("Keys() should be empty: %q", keys)
	}
	res, err := db.Put("test_key_1", "test_value_1", -1)
	check(err)
	res2, err := db.Get("test_key_1", -1)
	if !reflect.DeepEqual(res, res2) {
		t.Errorf("bad KeyValue result got %+v, expected %+v", res2, res)
	}
	res, err = db.Put("test_key_1", "test_value_1.1", -1)
	check(err)
	res2, err = db.Get("test_key_1", -1)
	if !reflect.DeepEqual(res, res2) {
		t.Errorf("bad KeyValue result got %+v, expected %+v", res2, res)
	}
	versions, err := db.Versions("test_key_1", 0, int(time.Now().UTC().UnixNano()), 0)
	check(err)
	if len(versions.Versions) != 2 {
		t.Errorf("key test_key_1 should have 2 versions, got %d", len(versions.Versions))
	}
	if versions.Versions[1].Value != res2.Value {
		t.Errorf("bad KeyValue result got %+v, expected %+v", versions.Versions[0].Value, res2.Value)
	}
	keys, err = db.Keys("", "\xff", 0)
	check(err)
	if len(keys) != 1 || keys[0] != res2.Key {
		t.Errorf("bad Keys() result, expected [%v], got %q", res2.Key, keys)
	}
}
