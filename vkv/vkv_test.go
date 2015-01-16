package vkv

import (
	"sync"
	"testing"
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
	// testing low level binary encoded uint32
	valUint, err := db.getUint32([]byte("foo2"))
	check(err)
	if valUint != uint32(0) {
		t.Errorf("Uninitialized uint32 key should return 0")
	}
	err = db.putUint32([]byte("foo2"), uint32(5))
	check(err)
	valUint, err = db.getUint32([]byte("foo2"))
	check(err)
	if valUint != uint32(5) {
		t.Error("Key should be set to 5")
	}
	err = db.incrUint32([]byte("foo2"), 2)
	check(err)
	valUint, err = db.getUint32([]byte("foo2"))
	check(err)
	if valUint != uint32(7) {
		t.Error("Key should be set to 7")
	}
	var wg sync.WaitGroup
	// Test the mutex
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err = db.incrUint32([]byte("foo3"), 1)
			check(err)
		}()
	}
	wg.Wait()
	valUint, err = db.getUint32([]byte("foo3"))
	check(err)
	if valUint != uint32(10) {
		t.Errorf("Key foo3 should be set to 10, got %v", valUint)
	}
	keys, err := db.Keys("", "\xff", 0)
	check(err)
	if len(keys) != 0 {
		t.Errorf("Keys() should be empty: %q", keys)
	}
}
