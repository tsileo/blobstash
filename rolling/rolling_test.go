package rolling

import (
	"crypto/rand"
	"io"
	"math"
	"testing"
)

func TestRandom(t *testing.T) {
	window := 256
	var blockSize uint32 = 1024 * 32
	var target uint32 = math.MaxUint32 / blockSize

	rs := New(window)
	for i := 0; i < 100000; i++ {
		io.CopyN(rs, rand.Reader, 1)
		if rs.Sum32() < target {
			t.Logf("sum at %v<target: %v, ratio=%v", i, rs.Sum32(), float64(rs.Sum32())/float64(1<<32))
		}
	}
}

func TestRollingSum(t *testing.T) {
	data1 := []byte("hello my name is joe and I work in a button factory")
	data2 := []byte("hello my name is joe and I eat in a button factory")
	window := 8

	rs := New(window)
	sums1 := []uint32{}
	for i, c := range data1 {
		rs.WriteByte(c)
		sums1 = append(sums1, rs.Sum32())
		t.Logf("sum1 at %v: %v", i, sums1[i])
	}

	rs.Reset()
	sums2 := []uint32{}
	for _, c := range data2 {
		rs.WriteByte(c)
		sums2 = append(sums2, rs.Sum32())
	}

	for i := 0; i < 27; i++ {
		if sums1[i] != sums2[i] {
			t.Errorf("pre sums %v don't match, %v != %v", i, sums1[i], sums2[i])
		}
	}

	for i := 0; i < 13; i++ {
		i1 := len(sums1) - i - 1
		i2 := len(sums2) - i - 1
		if sums1[i1] != sums2[i2] {
			t.Errorf("post sums %v don't match, %v != %v", i, sums1[i1], sums2[i2])
		}
	}
}
