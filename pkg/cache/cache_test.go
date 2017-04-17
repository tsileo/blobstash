package cache

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"os"
	"testing"
	"time"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func TestCacheFileStorage(t *testing.T) {
	cache, err := New(1000000)
	check(err)
	defer func() {
		cache.db.Close()
		os.RemoveAll("ok")
	}()

	t.Logf("cache=%v", cache)

	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("%d-ok", i)
		val := make([]byte, 500000)
		if _, err := rand.Reader.Read(val[:]); err != nil {
			panic(err)
		}

		check(cache.dbSet(key, val))
		val2, err := cache.dbGet(key)
		check(err)

		if !bytes.Equal(val, val2) {
			t.Errorf("big val error (%d/%d)", len(val), len(val2))
		}
	}
}

func TestCacheBasic(t *testing.T) {
	cache, err := New(1000000)
	check(err)
	defer func() {
		cache.db.Close()
		os.RemoveAll("ok")
	}()

	t.Logf("cache=%v", cache)

	val := []byte("value")
	cache.Add("key2", val)

	val2, ok, err := cache.Get("key2")
	check(err)
	if !ok {
		t.Errorf("key should exist")
	}
	if !bytes.Equal(val, val2) {
		t.Errorf("failed to retrieve data (%s/%s)", val, val2)
	}

	_, ok, err = cache.Get("key")
	check(err)
	if ok {
		t.Errorf("key \"key\" should not exist")
	}
}

func TestCacheLRU(t *testing.T) {
	maxSize := 1000000
	cache, err := New(maxSize)
	check(err)
	defer func() {
		cache.db.Close()
		os.RemoveAll("ok")
	}()

	t.Logf("cache=%v", cache)

	kvs := map[string][]byte{}
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("ok-%d", i)
		val := make([]byte, (maxSize/10)-1)
		if _, err := rand.Reader.Read(val[:]); err != nil {
			panic(err)
		}
		cache.Add(key, val)
		if i > 9 {
			kvs[key] = val
		}
	}

	if cache.currentSize > cache.maxSize {
		t.Errorf("should not exceed max size")
	}

	if len(cache.items) != 10 || len(kvs) != 10 {
		t.Errorf("should not contain more than 10 items")
	}

	for i := 0; i < 10; i++ {
		_, ok, err := cache.Get(fmt.Sprintf("ok-%d", i))
		check(err)
		if ok {
			t.Errorf("key \"ok-%d\" should have been evicted", i)
		}
	}

	for k, v := range kvs {
		start := time.Now()
		v2, _, err := cache.Get(k)
		t.Logf("cache.Get %s", time.Since(start))
		check(err)
		if !bytes.Equal(v, v2) {
			t.Errorf("key \"%s\" should be present", k)
		}
	}
}
