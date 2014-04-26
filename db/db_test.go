package db

import (
    "testing"
    "bytes"
    "sync"
)

func check(e error) {
    if e != nil {
        panic(e)
    }
}

func TestSuite(t *testing.T) {
	db, err := New("test_db")
	if err != nil {
		t.Fatal("Error creating db")
	}	

	err = db.put([]byte("foo"), []byte("bar"))
	check(err)

	val, err := db.get([]byte("foo"))
	check(err)
	
	if !bytes.Equal(val, []byte("bar")) {
		t.Errorf("Error getting value")
	}

	val, err = db.get([]byte("foo2"))
	check(err)
	
	if val != nil {
		t.Errorf("Non existent key should return nil")
	}

	// testing basic getset/incrby command
	val, err = db.getset([]byte("foo"), []byte("10"))
	check(err)
	if !bytes.Equal(val, []byte("bar")) {
		t.Error("Error during getset")
	}

	val, err = db.get([]byte("foo"))
	check(err)
	
	if !bytes.Equal(val, []byte("10")) {
		t.Errorf("Error getting value")
	}

	err = db.del([]byte("foo"))
	check(err)
		
	val, err = db.get([]byte("foo"))
	check(err)
	if val != nil {
		t.Error("Deleted key should return nil")
	}

	err = db.incrby([]byte("foo"), 50)
	check(err)

	val, err = db.get([]byte("foo"))
	check(err)
	if !bytes.Equal(val, []byte("50")) {
		t.Error("Error incrby")
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
	for i := 0; i < 50; i++ {
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
	if valUint != uint32(50) {
		t.Errorf("Key foo3 should be set to 10, got %v", valUint)
	}

	db.Close()
	err = db.Destroy()
	if err != nil {
		panic(err)
	}
}
