package docstore

import (
	"testing"

	"github.com/yuin/gopher-lua"
)

func TestLuaHook(t *testing.T) {
	L := lua.NewState()
	defer L.Close()

	h, err := NewLuaHook(L, `
function hook(doc)
  doc.hooked = true
  doc.count = doc.count + 1
  return doc
end
return hook`)
	if err != nil {
		panic(err)
	}

	doc := map[string]interface{}{
		"count": 1,
		"nested": map[string]interface{}{
			"works": true,
		},
	}
	doc2, err := h.Execute(doc)
	if err != nil {
		panic(err)
	}
	t.Logf("doc=%+v", doc2)
	if int(doc2["count"].(float64)) != 2 {
		t.Errorf("epxected count to be 2, got %v", doc2["count"].(float64))
	}
}
