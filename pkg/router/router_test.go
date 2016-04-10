package router

import (
	"encoding/json"
	"testing"
)

func check(err error) {
	if err != nil {
		panic(err)
	}
}

var testConf1 = `[
["if-ns-tomt0m", "customHandler"],
["default", "blobHandler"]
]`

var testConf2 = `[
["default", "blobHandler"]
]`

func decodeJsonRules(js string) ([]interface{}, error) {
	res := []interface{}{}
	err := json.Unmarshal([]byte(js), &res)
	return res, err
}

func TestRouter(t *testing.T) {
	// tConf1, err := decodeJsonRules(testConf1)
	// check(err)
	// routerConfig := New(tConf1)
	// tConf2, err := decodeJsonRules(testConf2)
	// check(err)
	// routerConfig = New(tConf2)
}
