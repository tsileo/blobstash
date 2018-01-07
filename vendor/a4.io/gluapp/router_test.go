package gluapp

import (
	"reflect"
	"testing"
)

// TODO(tsileo): add a bench

var testRoute = []struct {
	method, path, path2 string
	data, expectedData  string
	expectedParams      params
	expectedErr         error
}{
	{"GET", "/hello", "/hello", "hello", "hello", params{}, nil},
	{"PUT", "", "/hello", "", "", nil, errMethodNotAllowed},
	{"POST", "/hello", "/hello", "hellopost", "hellopost", params{}, nil},
	{"GET", "/", "/", "index", "index", params{}, nil},
	{"GET", "/hello/:name", "/hello/thomas", "hellop", "hellop", params{"name": "thomas"}, nil},
	{"GET", "/hello/ok", "/hello/ok", "hellok", "hellop", params{"name": "ok"}, nil},
	{"GET", "/another/page/:foo/:bar", "/another/page/lol/nope", "foobar", "foobar", params{"foo": "lol", "bar": "nope"}, nil},
	{"GET", "not:a named/parameter", "not:a named/parameter", "nnp", "nnp", params{}, nil},
	{"GET", "", "/foobar", "", "", nil, errNotFound},
}

func TestRouter(t *testing.T) {
	r := &router{routes: []*route{}}
	check := func(method, path, name string, pExpected params, eErr error) {
		t.Logf("testing %v %v %v", method, path, name)
		route, params, err := r.match(method, path)
		if err != eErr {
			t.Errorf("got %+v expected %v", err, eErr)
		}
		if route != nil && route.(string) != name {
			t.Errorf("got %+v expected \"%s\"", route.(string), name)
		}
		if (len(params) > 0 || len(pExpected) > 0) && !reflect.DeepEqual(params, pExpected) {
			t.Errorf("got %+v expected %+v", params, pExpected)
		}
	}
	// Create all routes first
	for _, testData := range testRoute {
		if testData.path != "" {
			r.add(testData.method, testData.path, testData.data)
		}
	}
	// Do the assertions
	for _, testData := range testRoute {
		check(testData.method, testData.path2, testData.expectedData, testData.expectedParams, testData.expectedErr)
	}
}
