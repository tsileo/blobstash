package docstore

import (
	"sync"
	"testing"
)

func TestLock(t *testing.T) {
	l := NewLocker()
	var wg sync.WaitGroup
	out := 0
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			l.Lock("deadbeef")
			out++
			l.Unlock("deadbeef")
			wg.Done()
		}(i)
	}
	wg.Wait()
	if out != 100 {
		t.Errorf("expected out to be %d, got %d", 100, out)
	}
}
