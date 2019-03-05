/*
Package textsearch implements basic text search features (for matching text fields of JSON documents).
*/
package textsearch // import "a4.io/blobstash/pkg/textsearch"

import (
	"bytes"
	"fmt"
	"strings"
	"text/scanner"

	"github.com/blevesearch/segment"
	lru "github.com/hashicorp/golang-lru"
	porterstemmer "github.com/reiver/go-porterstemmer"
)

// cache for `string` -> `SearchTerms`
var searchTermsCache, _ = lru.New(128)

// searchTerm holds a single search term
type searchTerm struct {
	prefix     string // `+` (for required match) or `-` (for excluding doc matching the term)
	term       string
	exactMatch bool // true if the search term was quoted for exact match
}

// SearchTerms holds a parsed text search query
type SearchTerms []*searchTerm

// IndexedDoc holds a parsed "document"
type IndexedDoc struct {
	Content string         `msgpack:"c"`
	Stems   map[string]int `msgpack:"s"`
}

// NewIndexedDoc returns a parsed "document"
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

// ParseTextQuery returns a parsed text query
func ParseTextQuery(q string) SearchTerms {
	if cached, ok := cache.Get(q); ok {
		fmt.Printf("ParseTextQuery form cache")
		return cached.(SearchTerms)
	}
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
	cache.Add(q, out)
	return out
}

// Match returns true if the query matches the given `IndexedDoc`
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
