package gluapp

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/yuin/gopher-lua"
)

func TestHTTP(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	}))
	defer server.Close()

	// Create a new empty state
	L := lua.NewState()
	defer L.Close()

	// Setup the state
	L.PreloadModule("http", setupHTTP(http.DefaultClient, ""))
	setupTestState(L, t)
	L.SetGlobal("server_url", lua.LString(server.URL))

	// Execute the Lua code
	if err := L.DoString(`
logf('test HTTP %d', 1)
http = require('http').new()
resp, err = http:get(server_url)
print(resp.headers:get('Date'))
for k,v in pairs(resp.headers:raw()) do
  print(k, resp.headers:get(k))
end
if resp.status_code ~= 200 then
  errorf('bad status code, expected 200, got %d', resp.status_code)
end
print(resp.body:size())
print(resp.body:text())
-- TODO(tsileo): make a test to ensure json is nil in this case
-- print(resp.body:json())
print(err)
`); err != nil {
		panic(err)
	}
}
