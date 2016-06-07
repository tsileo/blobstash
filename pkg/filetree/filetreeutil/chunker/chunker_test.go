package chunker

import (
	"fmt"
	"log"
	"math/rand"
	"testing"
)

func TestChunker(t *testing.T) {
	c := New()
	fmt.Printf("%+v", c)
}

func BenchmarkRollsum(b *testing.B) {
	const bufSize = 20 << 20
	buf := make([]byte, bufSize)
	for i := range buf {
		buf[i] = byte(rand.Int63())
	}

	b.ResetTimer()
	c := New()
	splits := 0
	for i := 0; i < b.N; i++ {
		splits = 0
		for ii, b := range buf {
			c.WriteByte(b)
			if c.OnSplit() {
				log.Printf("Split at %v / block size=%v", ii, c.BlockSize)
				c.Reset()
				splits++
			}
		}
	}
	b.SetBytes(bufSize)
	b.Logf("num splits = %d; every %d bytes", splits, int(float64(bufSize)/float64(splits)))
}
