package gluapp

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestApp(t *testing.T) {
	app, err := NewApp(&Config{Path: "tests_data/app/"})
	if err != nil {
		panic(err)
	}

	server := httptest.NewServer(app)
	defer server.Close()

	testData := []struct {
		server                     *httptest.Server
		method                     string
		path                       string
		body                       bytes.Buffer
		expectedResponseBody       string
		expectedResponseStatusCode int
	}{
		// Ensure the app get called
		{
			method:                     "GET",
			server:                     server,
			path:                       "/",
			expectedResponseBody:       "hello app",
			expectedResponseStatusCode: 200,
		},
		// Ensure the app get called
		{
			method:                     "GET",
			server:                     server,
			path:                       "/bar",
			expectedResponseBody:       "bar",
			expectedResponseStatusCode: 200,
		},
		// Ensure files from public/ directory are served
		{
			method:                     "GET",
			server:                     server,
			path:                       "/lol.html",
			expectedResponseBody:       "lol\n",
			expectedResponseStatusCode: 200,
		},
		// Ensure a 404 get triggered on undefined route
		{
			method:                     "GET",
			server:                     server,
			path:                       "/foo.html",
			expectedResponseBody:       "Not Found",
			expectedResponseStatusCode: 404,
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
