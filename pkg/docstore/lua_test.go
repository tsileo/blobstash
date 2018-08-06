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

	if err := mre.SetupMap(`
	function map(doc)
	  emit("data", { count = doc.count })
	end
	return map`); err != nil {
		panic(err)
	}

	if err := mre.SetupReduce(`
	function reduce(key, docs)
	  local out = { count = 0 }
	  for i, doc in ipairs(docs) do
		out.count = out.count + doc.count
	  end
	  return out
	end
	return reduce`); err != nil {
		panic(err)
	}

	mre2, _ := mre.Duplicate()
	defer mre2.Close()

	doc := map[string]interface{}{
		"count": 1,
		"nested": map[string]interface{}{
			"works": true,
		},
	}
	doc2 := map[string]interface{}{
		"count": 2,
	}

	for _, d := range []map[string]interface{}{doc, doc, doc} {
		if err := mre.Map(d); err != nil {
			panic(err)
		}
	}

	t.Logf("mre emitted=%+v\n", mre.emitted)
	for _, d := range []map[string]interface{}{doc2, doc2, doc2} {
		if err := mre2.Map(d); err != nil {
			panic(err)
		}
	}
	t.Logf("mre2 emitted=%+v\n", mre2.emitted)

	if err := mre.Reduce(nil); err != nil {
		panic(err)
	}

	result, err := mre.Finalize()
	if err != nil {
		panic(err)
	}
	t.Logf("mre result=%+v\n", result)
	if int(result["data"]["count"].(float64)) != 3 {
		t.Errorf("expected 3, got %d\n", result["data"]["count"])
	}

	if err := mre2.Reduce(nil); err != nil {
		panic(err)
	}

	result2, err := mre2.Finalize()
	if err != nil {
		panic(err)
	}
	t.Logf("mre2 result=%+v\n", result2)
	if int(result2["data"]["count"].(float64)) != 6 {
		t.Errorf("expected 6, got %d\n", result2["data"]["count"])
	}

	if err := mre.Reduce(mre2); err != nil {
		panic(err)
	}
	result3, err := mre.Finalize()
	if err != nil {
		panic(err)
	}
	t.Logf("merge result=%+v\n", result3)
	if int(result3["data"]["count"].(float64)) != 9 {
		t.Errorf("expected 9, got %d\n", result3["data"]["count"])
	}
}
