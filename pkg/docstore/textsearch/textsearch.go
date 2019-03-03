package textsearch // import "a4.io/blobstash/pkg/textsearch"

import (
	"bytes"
	"strings"
	"text/scanner"

	"github.com/blevesearch/segment"
	porterstemmer "github.com/reiver/go-porterstemmer"
)

type searchTerm struct {
	prefix     string
	term       string
	exactMatch bool
}

type SearchTerms []*searchTerm

type IndexedDoc struct {
	Content string         `msgpack:"c"`
	Stems   map[string]int `msgpack:"s"`
}

func NewIndexedDoc(doc map[string]interface{}, fields []string) (*IndexedDoc, error) {
	parts := []string{}
	stems := map[string]int{}
	for _, field := range fields {
		if dat, ok := doc[field]; ok {
			parts = append(parts, dat.(string))
			segmenter := segment.NewWordSegmenter(bytes.NewReader([]byte(dat.(string))))
			for segmenter.Segment() {
				if segmenter.Type() == segment.Letter {
					stem := porterstemmer.StemString(segmenter.Text())
					if _, ok := stems[stem]; ok {
						stems[stem] += 1
					} else {
						stems[stem] = 1
					}
				}
			}
			if err := segmenter.Err(); err != nil {
				return nil, err
			}
		}
	}
	content := strings.Join(parts, " ")

	return &IndexedDoc{Content: content, Stems: stems}, nil
}

func (terms SearchTerms) Match(d *IndexedDoc) bool {
	match := false

	for _, st := range terms {
		cond := false
		switch {
		case st.exactMatch:
			cond = strings.Contains(d.Content, st.term)
		default:
			_, cond = d.Stems[st.term]
		}

		if st.prefix == "+" {
			if !cond {
				return false
			}
		} else if st.prefix == "-" {
			if cond {
				return false
			} else {
				match = true
			}
		}

		if cond {
			match = true
		}
	}

	return match
}

func ParseTextQuery(q string) SearchTerms {
	var s scanner.Scanner
	s.Init(strings.NewReader(q))
	out := SearchTerms{}
	var prefix, term string
	var exactMatch bool
	for tok := s.Scan(); tok != scanner.EOF; tok = s.Scan() {
		term = s.TokenText()

		if term == "+" || term == "-" {
			prefix = term
			continue
		}

		if strings.HasPrefix(term, "\"") && strings.HasSuffix(term, "\"") {
			exactMatch = true
			term = term[1 : len(term)-1]
		}

		if !exactMatch {
			term = porterstemmer.StemString(term)
		}

		out = append(out, &searchTerm{
			prefix:     prefix,
			term:       term,
			exactMatch: exactMatch,
		})

		prefix = ""
		exactMatch = false
	}
	return out
}

//func main() {
//	d, err := NewIndexedDoc(map[string]interface{}{"content": "hello thomas and girls"}, []string{"content"})
//	if err != nil {
//		panic(err)
//	}
//	out := ParseTextQuery(src)
//	res := out.Match(d)
//}
