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

func TestLuaMapReduce(t *testing.T) {
	mre := NewMapReduceEngine()
	defer mre.Close()

	h, err := NewLuaHook(mre.L, `
function map(doc)
  emit("data", { count = doc.count })
end
return map`)
	if err != nil {
		panic(err)
	}
	mre.M = h

	h2, err := NewLuaHook(mre.L, `
function reduce(key, docs)
  local out = { count = 0 }
  for i, doc in ipairs(docs) do
    out.count = out.count + doc.count
  end
  return out
end
return reduce`)
	if err != nil {
		panic(err)
	}
	mre.R = h2

	doc := map[string]interface{}{
		"count": 1,
		"nested": map[string]interface{}{
			"works": true,
		},
	}

	for _, doc := range []map[string]interface{}{doc, doc, doc} {
		if err := mre.Map(doc); err != nil {
			panic(err)
		}
	}

	t.Logf("emitted=%+v\n", mre.emitted)

	if err := mre.Reduce(); err != nil {
		panic(err)
	}

	result, err := mre.Finalize()
	if err != nil {
		panic(err)
	}
	t.Logf("result=%+v\n", result)
	if int(result["data"]["count"].(float64)) != 3 {
		t.Errorf("expected 3, got %d\n", result["data"]["count"])
	}
}
