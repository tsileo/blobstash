package docstore

import (
	"testing"
)

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
