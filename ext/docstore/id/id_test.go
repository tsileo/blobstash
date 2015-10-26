package id

import (
	"encoding/json"
	"testing"
)

func check(err error) {
	if err != nil {
		panic(err)
	}
}

var (
	testTs   = 1429882340
	testHash = "c0f1480a26c2fd4deb8e738a52b7530ed111b9bcd17bbb09259ce03f129988c5"
)

func TestID(t *testing.T) {
	t.Log("Testing encoding...")
	id, err := New(testTs, testHash)
	check(err)
	t.Logf("cursor: %+v", id)
	if id.Ts() != testTs {
		t.Errorf("unexpected ts, got %v, expected %v", id.Ts(), testTs)
	}
	rhash, err := id.Hash()
	check(err)
	if rhash != testHash {
		t.Errorf("unexpected hash, got %v, expected %v", rhash, testHash)
	}

	t.Logf("Testing JSON (un)marshalling...")
	js, err := json.Marshal(id)
	check(err)
	t.Logf("marshalled id: %v", string(js))
	id2 := &ID{}
	if err := json.Unmarshal(js, id2); err != nil {
		panic(err)
	}
	if id2.String() != id.String() {
		t.Errorf("failed to unmarshal cursor, got %v, expected %v", id2.String(), id.String())
	}
	t.Logf("Testing decoding from hex...")
	id3, err := FromHex(id.String())
	check(err)
	rhash3, err := id3.Hash()
	check(err)
	if id3.String() != id.String() || id3.Ts() != id.Ts() || rhash3 != rhash {
		t.Errorf("failed to decode from hex string")
	}
}
