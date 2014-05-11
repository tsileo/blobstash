package disklru

import (
	"testing"
	"os"
	"bytes"
	"time"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func LRUTestFunc(key string) []byte {
	return []byte(key)
}

var fakeData = []struct{
	key string
	data []byte
	fetched bool
	size uint32
	cnt uint32
}{
	{"testget", []byte("testget"), true, 7, 1},
	{"testget", []byte("testget"), false, 7, 1},
	{"testget2", []byte("testget2"), true, 15, 2},
	{"aaaaaaaaaaaaaaaaaaaa", []byte("aaaaaaaaaaaaaaaaaaaa"), true, 35, 3},
	{"bbbbbbbbbbbbbbbbbbbb", []byte("bbbbbbbbbbbbbbbbbbbb"), true, 48, 3},
	{"aaaaaaaaaaaaaaaaaaaa", []byte("aaaaaaaaaaaaaaaaaaaa"), false, 48, 3},
	{"cccccccccccccccccccc", []byte("cccccccccccccccccccc"), true, 40, 2},
	{"bbbbbbbbbbbbbbbbbbbb", []byte("bbbbbbbbbbbbbbbbbbbb"), true, 40, 2},
	{"cccccccccccccccccccc", []byte("cccccccccccccccccccc"), false, 40, 2},
	{"testget", []byte("testget"), true, 47, 3},
}

func TestDiskLRU(t *testing.T) {
	lru, err := New("tmp_lru_test", LRUTestFunc, 50)
	check(err)
	defer os.RemoveAll("tmp_lru_test")

	for _, tdata := range fakeData {
		data, fetched, err := lru.Get(tdata.key)
		check(err)
		if !bytes.Equal(data, tdata.data) {
			t.Errorf("Bad get result, got:%+v, expected:%+v", data, tdata.data)
		}
		if fetched != tdata.fetched {
			t.Errorf("Bad get result, fetched=%v, expected fetched=%+v", fetched, tdata)
		}
		if lru.Size() != tdata.size {
			t.Errorf("Bad DiskLRU size, got:%v, expected:%v", lru.Size(), tdata.size)
		}
		if lru.Cnt() != tdata.cnt {
			t.Errorf("Bad DiskLRU items count, got:%v, expected:%v", lru.Cnt(), tdata.cnt)
		}
		time.Sleep(500 * time.Millisecond)
	}	

}
