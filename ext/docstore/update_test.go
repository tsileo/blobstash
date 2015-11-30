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
			"k2": false,
		},
	})
	check(err)
	if newDoc["k2"] != false {
		t.Errorf("\"k2\" should be `true`")
	}
}
