package router

import (
	"testing"

	"github.com/karlseguin/typed"
)

func check(err error) {
	if err != nil {
		panic(err)
	}
}

var testConf1 = `[
[["if-ns-tomt0m", "if-meta"], "customHandler2"],
["if-ns-tomt0m", "customHandler"],
["if-meta", "metaHandler"],
["default", "blobHandler"]
]`

var testConfData1 = []struct {
	req             *Request
	expectedBackend string
}{
	{&Request{Read, false, "tomt0m"}, "customHandler"},
	{&Request{Read, true, "tomt0m"}, "customHandler2"},
	{&Request{Read, true, "homeserver"}, "metaHandler"},
	{&Request{Read, false, "homeserver"}, "blobHandler"},
	{&Request{Read, true, "homeserver"}, "metaHandler"},
	{&Request{Read, false, "homeserver"}, "blobHandler"},
}

var testConf2 = `[
["default", "blobHandler"]
]`

var testConfData2 = []struct {
	req             *Request
	expectedBackend string
}{
	{&Request{Read, false, "homeserver"}, "blobHandler"},
}

func TestRouter(t *testing.T) {
	tConf1, err := typed.JsonStringArray(testConf1)
	check(err)
	routerConfig := New(tConf1)
	for _, tdata := range testConfData1 {
		backend := routerConfig.Route(tdata.req)
		if backend != tdata.expectedBackend {
			t.Errorf("Bad routing result for req %+v, expected:%v, got:%v", tdata.req, tdata.expectedBackend, backend)
		}
	}
	tConf2, err := typed.JsonStringArray(testConf2)
	check(err)
	routerConfig = New(tConf2)
	check(err)
	for _, tdata := range testConfData2 {
		backend := routerConfig.Route(tdata.req)
		if backend != tdata.expectedBackend {
			t.Errorf("Bad routing result for req %+v, expected:%v, got:%v", tdata.req, tdata.expectedBackend, backend)
		}
	}
}
