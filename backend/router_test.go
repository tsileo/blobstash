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
["if-host-tomt0m", "customHandler"],
["if-meta", "metaHandler"],
["default", "blobHandler"]
]`

var testConfData1 = []struct{
	req *Request
	expectedBackend string
}{
	{NewReadRequest("tomt0m", "hashtoread", false), "customHandler2"},
}

func TestRouter(t *testing.T) {
	routerConfig, err := NewRouterConfig([]byte(testConf1))
	check(err)
	for _, tdata := range testConfData1 {
		backend := routerConfig.Route(tdata.req)
		if backend != tdata.expectedBackend {
			t.Errorf("Bad routing result for req %+v, expected:%v, got:%v", tdata.req, tdata.expectedBackend, backend)
		}
	}
}
