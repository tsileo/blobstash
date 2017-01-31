package index

import (
	"os"
	"testing"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func TestIndex(t *testing.T) {
	i, err := New("index_test")
	defer func() {
		i.Close()
		os.Remove("index_test")
	}()
	if err != nil {
		t.Fatalf("Error creating db %v", err)
	}
	h := "c0f1480a26c2fd4deb8e738a52b7530ed111b9bcd17bbb09259ce03f12998800"
	h2 := "c0f1480a26c2fd4deb8e738a52b7530ed111b9bcd17bbb09259ce03f129988c5"
	check(i.Index(h))
	ok, err := i.Exists(h)
	check(err)
	if !ok {
		t.Errorf("h \"%s\" should exists", h)
	}
	ok2, err := i.Exists(h2)
	check(err)
	if ok2 {
		t.Errorf("h \"%s\" should not exists", h2)
	}
}
