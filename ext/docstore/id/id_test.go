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
	testTs = 1429882340
)

func TestID(t *testing.T) {
	t.Log("Testing encoding...")
	id, err := New(testTs)
	check(err)
	t.Logf("cursor: %+v", id)
	if id.Ts() != testTs {
		t.Errorf("unexpected ts, got %v, expected %v", id.Ts(), testTs)
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
	if id3.String() != id.String() || id3.Ts() != id.Ts() {
		t.Errorf("failed to decode from hex string")
	}
}
