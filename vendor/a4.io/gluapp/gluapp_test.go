package gluapp

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/yuin/gopher-lua"
)

// logf will allow to call t.Logf directly from Lua code
func logf(t *testing.T) func(*lua.LState) int {
	return func(L *lua.LState) int {
		var args []interface{}
		for i := 2; i <= L.GetTop(); i++ {
			args = append(args, L.CheckAny(i))
		}
		t.Logf(string(L.ToString(1)), args...)
		return 0
	}
}

// errorf will allow to call t.Logf directly from Lua code
func errorf(t *testing.T) func(*lua.LState) int {
	return func(L *lua.LState) int {
		var args []interface{}
		for i := 2; i <= L.GetTop(); i++ {
			args = append(args, L.CheckAny(i))
		}
		t.Errorf(string(L.ToString(1)), args...)
		return 0
	}
}

func setupTestState(L *lua.LState, t *testing.T) {
	setupMetatable(L)
	L.SetGlobal("logf", L.NewFunction(logf(t)))
	L.SetGlobal("errorf", L.NewFunction(errorf(t)))
}

var testApp1 = `
app.response:write('Hello World!')
`

var testAppRouter1 = `
router = require('router').new()
router:get('/hello/:name', function(params)
  app.response:write('hello ' .. params.name)
  print('hello')
end)
router:run()
`

func TestExec(t *testing.T) {
	h1 := func(w http.ResponseWriter, r *http.Request) {
		if err := Exec(&Config{}, testApp1, w, r); err != nil {
			panic(err)
		}
	}

	h2 := func(w http.ResponseWriter, r *http.Request) {
		if err := Exec(&Config{}, testAppRouter1, w, r); err != nil {
			panic(err)
		}
	}

	servers := map[string]*httptest.Server{
		"s1": httptest.NewServer(http.HandlerFunc(h1)),
		"s2": httptest.NewServer(http.HandlerFunc(h2)),
	}

	for _, server := range servers {
		defer server.Close()
	}

	// TODO(tsileo): also test headers
	testData := []struct {
		server                     *httptest.Server
		method                     string
		path                       string
		body                       bytes.Buffer
		expectedResponseBody       string
		expectedResponseStatusCode int
	}{
		{
			method:                     "GET",
			server:                     servers["s1"],
			path:                       "/",
			expectedResponseBody:       "Hello World!",
			expectedResponseStatusCode: 200,
		},
		{
			method:                     "GET",
			server:                     servers["s1"],
			path:                       "/foo",
			expectedResponseBody:       "Hello World!",
			expectedResponseStatusCode: 200,
		},
		{
			method:                     "GET",
			server:                     servers["s2"],
			path:                       "/hello/thomas/",
			expectedResponseBody:       "hello thomas",
			expectedResponseStatusCode: 200,
		},
	}

	for _, tdata := range testData {
		var resp *http.Response
		var err error
		switch tdata.method {
		case "GET":
			resp, err = http.Get(tdata.server.URL + tdata.path)
			if err != nil {
				panic(err)
			}
		}
		body, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			panic(err)
		}
		t.Logf("body=%s\n", body)
		if resp.StatusCode != tdata.expectedResponseStatusCode {
			t.Errorf("bad status code, got %d, expected %d", resp.StatusCode, tdata.expectedResponseStatusCode)
		}
		if string(body) != tdata.expectedResponseBody {
			t.Errorf("bad body, got %s, expected %s", body, tdata.expectedResponseBody)
		}
	}
}
