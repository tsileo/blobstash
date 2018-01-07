package gluapp

import (
	"testing"

	"github.com/yuin/gopher-lua"
)

func TestJSON(t *testing.T) {
	// Create a new empty state
	L := lua.NewState()
	defer L.Close()

	// Setup the state
	L.PreloadModule("json", loadJSON)
	setupTestState(L, t)

	// Execute the Lua code
	if err := L.DoString(`
json = require('json')

data = {hello = 'world', num = 42, trueorfalse = true}

out = json.encode(data)

expected = [[{"hello":"world","num":42,"trueorfalse":true}]]

-- TODO(tsileo): better check
decoded = json.decode(expected)
if decoded ~= data then
  for k,v in pairs(decoded) do
    if data[k] ~= v then
      errorf('json.decode error, got %v, expected %v', v, data[k])
    end
  end
end

if out ~= expected then
  errorf('json.encode error, got %v, expected %v', out, expected)
end
`); err != nil {
		panic(err)
	}
}
