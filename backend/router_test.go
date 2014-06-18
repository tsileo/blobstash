package backend

import (
	"testing"
)

func check(err error) {
	if err != nil {
		panic(err)
	}
}

var testConf1 = `[
[["if-host-tomt0m", "if-meta"], "customHandler2"],
["if-host-tomt0m", "customHandler"],
["if-meta", "metaHandler"],
["default", "blobHandler"]
]`

var testConfData1 = []struct{
	req *Request
	expectedBackend string
}{
	{NewReadRequest("tomt0m", "hashtoread", false), "customHandler"},
	{NewReadRequest("tomt0m", "hashtoread", true), "customHandler2"},
}

var testConf2 = `[
["default", "blobHandler"]
]`

var testConfData2 = []struct{
	req *Request
	expectedBackend string
}{
	{NewReadRequest("tomt0m", "hashtoread", false), "blobHandler"},
}

func TestRouter(t *testing.T) {
	routerConfig, err := NewRouterFromConfig([]byte(testConf1))
	check(err)
	for _, tdata := range testConfData1 {
		backend := routerConfig.Route(tdata.req)
		if backend != tdata.expectedBackend {
			t.Errorf("Bad routing result for req %+v, expected:%v, got:%v", tdata.req, tdata.expectedBackend, backend)
		}
	}
	routerConfig, err = NewRouterFromConfig([]byte(testConf2))
	check(err)
	for _, tdata := range testConfData2 {
		backend := routerConfig.Route(tdata.req)
		if backend != tdata.expectedBackend {
			t.Errorf("Bad routing result for req %+v, expected:%v, got:%v", tdata.req, tdata.expectedBackend, backend)
		}
	}
}

// TODO RouterConfig to Router and NewRouterConfig to NewRouterFromConfig
