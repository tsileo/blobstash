package textsearch

import (
	"reflect"
	"testing"
)

type docMatch struct {
	doc   map[string]interface{}
	match bool
}

func TestParseTextQuery(t *testing.T) {
	for _, tdata := range []struct {
		qs       string
		expected SearchTerms
	}{
		{
			"girls",
			SearchTerms{
				&searchTerm{term: "girl"}, // XXX: girls has been stemmed
			},
		},
		{
			"lol ok",
			SearchTerms{
				&searchTerm{term: "lol"},
				&searchTerm{term: "ok"},
			},
		},
		{
			"\"lol ok\"",
			SearchTerms{
				&searchTerm{term: "lol ok", exactMatch: true},
			},
		},
		{
			"+lol -ok yes",
			SearchTerms{
				&searchTerm{term: "lol", prefix: "+"},
				&searchTerm{term: "ok", prefix: "-"},
				&searchTerm{term: "ye"}, // XXX: yes has been stemmed
			},
		},
	} {
		terms := ParseTextQuery(tdata.qs)
		if !reflect.DeepEqual(terms, tdata.expected) {
			t.Errorf("failed to split \"%s\", got %+v, expected %+v", tdata.qs, terms, tdata.expected)
		}
	}
}

func TestMatchSearchTerms(t *testing.T) {
	for _, tdata := range []struct {
		doc    map[string]interface{}
		qs     string
		fields []string
		match  bool
	}{
		{
			map[string]interface{}{"content": "girls"},
			"girl",
			[]string{},
			false,
		},
		{
			map[string]interface{}{"content": "girls"},
			"girl",
			[]string{"content"},
			true,
		},
		{
			map[string]interface{}{"content": "hello thomas"},
			"hello -thomas",
			[]string{"content"},
			false,
		},
		{
			map[string]interface{}{"content": "hello thomas"},
			"+thomas goodbye",
			[]string{"content"},
			true,
		},
		{
			map[string]interface{}{"content": "hello thomas"},
			"hello goodbye",
			[]string{"content"},
			true,
		},
		{
			map[string]interface{}{"content": "hello thomas"},
			"\"hello tho\" lol",
			[]string{"content"},
			true,
		},
		{
			map[string]interface{}{"content": "hello thomas"},
			"\"hello je\"",
			[]string{"content"},
			false,
		},
	} {
		t.Logf("tdata=%+v\n", tdata)
		idoc, err := NewIndexedDoc(tdata.doc, tdata.fields)
		if err != nil {
			panic(err)
		}
		terms := ParseTextQuery(tdata.qs)
		match := terms.Match(idoc)
		if match != tdata.match {
			t.Errorf("doc %+v expected to result in match=%v, got %v to work against %q", tdata.doc, tdata.match, match, tdata.qs)
		}
	}

}
