package blobsfile

import (
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/dchest/blake2b"
)

func TestBlobsIndexBasic(t *testing.T) {
	index, err := newIndex("tmp_test_index")
	check(err)
	defer index.Close()
	defer os.RemoveAll("tmp_test_index")

	bp := &blobPos{n: 1, offset: 5, size: 10, blobSize: 10}
	h := fmt.Sprintf("%x", blake2b.Sum256([]byte("fakehash")))
	err = index.setPos(h, bp)
	check(err)
	bp3, err := index.getPos(h)
	if bp.n != bp3.n || bp.offset != bp3.offset || bp.size != bp3.size || bp.blobSize != bp3.blobSize {
		t.Errorf("index.GetPos error, expected:%q, got:%q", bp, bp3)
	}

	err = index.setN(5)
	check(err)
	n2, err := index.getN()
	check(err)
	if n2 != 5 {
		t.Errorf("Error GetN, got %v, expected 5", n2)
	}
	err = index.setN(100)
	check(err)
	n2, err = index.getN()
	check(err)
	if n2 != 100 {
		t.Errorf("Error GetN, got %v, expected 100", n2)
	}

	for i := 0; i < 100; i++ {
		err = index.incInt64("ok", 100)
		check(err)
	}
	res, err := index.getInt64("ok")
	check(err)
	if res != 10000 {
		t.Errorf("expected 10000, got %d", res)
	}
}

func TestBlobsIndex(t *testing.T) {
	index, err := newIndex("tmp_test_index")
	check(err)
	defer index.Close()
	defer os.RemoveAll("tmp_test_index")
	var wg sync.WaitGroup
	var mu sync.Mutex
	expected := map[string]*blobPos{}
	for i := 0; i < 50000; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			data := fmt.Sprintf("fakehash %d", i)
			h := fmt.Sprintf("%x", blake2b.Sum256([]byte(data)))
			bp := &blobPos{n: 1, offset: 100, size: len(data), blobSize: len(data)}
			if err := index.setPos(h, bp); err != nil {
				panic(fmt.Errorf("failed to index.setPos a i=%d: %v", i, err))
			}
			mu.Lock()
			expected[h] = bp
			mu.Unlock()
		}(i)
	}

	wg.Wait()

	for h, ebp := range expected {
		bp, err := index.getPos(h)
		if err != nil {
			panic(fmt.Errorf("failed to index.getPos(\"%s\"): %v", h, err))
		}
		if bp.n != ebp.n || bp.offset != ebp.offset || bp.size != ebp.size || bp.blobSize != ebp.blobSize {
			t.Errorf("index.getPos error, expected:%q, got:%q", bp, ebp)
		}
	}
}

func BenchmarkBlobsIndex(b *testing.B) {
	index, err := newIndex("tmp_test_index")
	check(err)
	defer index.Close()
	defer os.RemoveAll("tmp_test_index")
	b.ResetTimer()
	b.StopTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		data := fmt.Sprintf("fakehash %d", i)
		h := fmt.Sprintf("%x", blake2b.Sum256([]byte(data)))
		bp := &blobPos{n: 1, offset: int64(100 + (i * len(data))), size: len(data), blobSize: len(data)}
		b.StartTimer()
		if err := index.setPos(h, bp); err != nil {
			panic(fmt.Errorf("failed to index.setPos a i=%d: %v", i, err))
		}
		b.StopTimer()
	}
}
