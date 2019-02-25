package index

import (
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
		i.Remove()
	}()
	if err != nil {
		t.Fatalf("Error creating db %v", err)
	}
	h := "c0f1480a26c2fd4deb8e738a52b7530ed111b9bcd17bbb09259ce03f12998800"
	h2 := "c0f1480a26c2fd4deb8e738a52b7530ed111b9bcd17bbb09259ce03f129988c5"
	h3 := "c0f1480a26c2fd4deb8e738a52b7530ed111b9bcd17bbb09259ce03f129988c5"
	check(i.Index(h, h2))
	ok, err := i.Exists(h)
	check(err)
	if !ok {
		t.Errorf("h \"%s\" should exists", h)
	}
	ehash, err := i.Get(h)
	check(err)
	if ehash != h2 {
		t.Errorf("failed to retrieve encrypted hash, expected %q, got %q", ehash, h2)
	}
	ok2, err := i.Exists(h3)
	check(err)
	if ok2 {
		t.Errorf("h \"%s\" should not exists", h2)
	}
}
