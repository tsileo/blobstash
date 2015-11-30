package docstore

import (
	"testing"
)

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func TestUpdate(t *testing.T) {
	testDoc := map[string]interface{}{
		"k1": 1,
		"k2": true,
		"k3": map[string]interface{}{
			"k31": "testing...",
		},
	}
	newDoc, err := updateDoc(testDoc, map[string]interface{}{
		"$set": map[string]interface{}{
			"k2":     false,
			"k3.k31": "tested",
			"k3.k32": "just added",
		},
	})
	check(err)
	if newDoc["k2"] != false {
		t.Errorf("\"k2\" should be `true`")
	}
	if newDoc["k3"].(map[string]interface{})["k31"] != "tested" {
		t.Errorf("\"k3.k31\" should be \"tested\", got %v", newDoc["k3"].(map[string]interface{})["k31"])
	}
}
