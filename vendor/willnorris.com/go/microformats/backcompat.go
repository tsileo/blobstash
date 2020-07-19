// Copyright (c) 2015 Andy Leap, Google
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to
// deal in the Software without restriction, including without limitation the
// rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
// sell copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
// IN THE SOFTWARE.

// This file includes backwards compatibility support for microformats v1.

package microformats

import (
	"net/url"
	"path"
	"strings"

	"golang.org/x/net/html"
	"golang.org/x/net/html/atom"
)

var (
	// map microformats v1 root classes to their v2 equivalent.
	backcompatRootMap = map[string]string{
		"adr":               "h-adr",
		"geo":               "h-geo",
		"hentry":            "h-entry",
		"hfeed":             "h-feed",
		"hnews":             "h-news",
		"hproduct":          "h-product",
		"hrecipe":           "h-recipe",
		"hresume":           "h-resume",
		"hreview":           "h-review",
		"hreview-aggregate": "h-review-aggregate",
		"vcard":             "h-card",
		"vevent":            "h-event",
	}

	// map microformats v1 property classes to their v2 equivalent. These
	// mappings are root-specific.
	backcompatPropertyMap = map[string]map[string]string{
		"h-adr": {
			"country-name":     "p-country-name",
			"extended-address": "p-extended-address",
			"locality":         "p-locality",
			"post-office-box":  "p-post-office-box",
			"postal-code":      "p-postal-code",
			"region":           "p-region",
			"street-address":   "p-street-address",
		},
		"h-card": {
			"additional-name":  "p-additional-name",
			"adr":              "p-adr",
			"agent":            "p-agent",
			"bday":             "dt-bday",
			"category":         "p-category",
			"class":            "p-class",
			"email":            "u-email",
			"family-name":      "p-family-name",
			"fn":               "p-name",
			"geo":              "p-geo",
			"given-name":       "p-given-name",
			"honorific-prefix": "p-honorific-prefix",
			"honorific-suffix": "p-honorific-suffix",
			"key":              "u-key",
			"label":            "p-label",
			"logo":             "u-logo",
			"mailer":           "p-mailer",
			"nickname":         "p-nickname",
			"note":             "p-note",
			"org":              "p-org",
			"photo":            "u-photo",
			"rev":              "dt-rev",
			"role":             "p-role",
			"sort-string":      "p-sort-string",
			"sound":            "u-sound",
			"tel":              "p-tel",
			"title":            "p-job-title",
			"tz":               "p-tz",
			"uid":              "u-uid",
			"url":              "u-url",
		},
		"h-entry": {
			"author":        "p-author",
			"category":      "p-category",
			"entry-content": "e-content",
			"entry-summary": "p-summary",
			"entry-title":   "p-name",
			"published":     "dt-published",
			"summary":       "p-summary",
			"updated":       "dt-updated",
		},
		"h-event": {
			"attendee":    "p-attendee",
			"category":    "p-category",
			"description": "p-description",
			"dtend":       "dt-end",
			"dtstart":     "dt-start",
			"duration":    "dt-duration",
			"geo":         "p-geo",
			"location":    "p-location",
			"summary":     "p-name",
			"url":         "u-url",
		},
		"h-feed": {
			"author": "p-author",
			"entry":  "p-entry",
			"photo":  "u-photo",
			"url":    "u-url",
		},
		"h-geo": {
			"latitude":  "p-latitude",
			"longitude": "p-longitude",
		},
		"h-item": {
			"fn":    "p-name",
			"photo": "u-photo",
			"url":   "u-url",
		},
		"h-news": {
			"dateline":   "p-dateline",
			"entry":      "p-entry",
			"geo":        "p-geo",
			"source-org": "p-source-org",
		},
		"h-product": {
			"brand":       "p-brand",
			"category":    "p-category",
			"description": "p-description",
			"fn":          "p-name",
			"identifier":  "u-identifier",
			"listing":     "p-listing",
			"photo":       "u-photo",
			"price":       "p-price",
			"review":      "p-review",
			"url":         "u-url",
		},
		"h-recipe": {
			"author":       "p-author",
			"category":     "p-category",
			"duration":     "dt-duration",
			"fn":           "p-name",
			"ingredient":   "p-ingredient",
			"instructions": "e-instructions",
			"nutrition":    "p-nutrition",
			"photo":        "u-photo",
			"summary":      "p-summary",
			"yield":        "p-yield",
		},
		"h-resume": {
			"affiliation":  "p-affiliation",
			"contact":      "p-contact",
			"education":    "p-education",
			"experience":   "p-experience",
			"publications": "p-publications",
			"skill":        "p-skill",
			"summary":      "p-summary",
		},
		"h-review": {
			"best":        "p-best",
			"description": "e-content",
			"dtreviewed":  "dt-reviewed",
			"item":        "p-item",
			"rating":      "p-rating",
			"reviewer":    "p-author",
			"summary":     "p-name",
			"worst":       "p-worst",
		},
		"h-review-aggregate": {
			"average": "p-average",
			"best":    "p-best",
			"count":   "p-count",
			"item":    "p-item",
			"rating":  "p-rating",
			"summary": "p-name",
			"votes":   "p-votes",
			"worst":   "p-worst",
		},
	}

	// map microformats v1 rel values to their v2 property equivalent. These
	// mappings are root-specific.
	backcompatRelMap = map[string]map[string]string{
		"h-card": {
			"tag": "u-category",
		},
		"h-entry": {
			"bookmark": "u-url",
			"tag":      "u-category",
		},
		"h-feed": {
			"tag": "u-category",
		},
		"h-news": {
			"principles": "u-principles",
		},
		"h-recipe": {
			"tag": "u-category",
		},
		"h-review": {
			"bookmark": "u-url",
			"tag":      "u-category",
		},
	}
)

// backcompatRootClasses returns the v2 root classes for the backcompat v1
// roots in the provided classes. parent identifies the parent microformat, if
// present, since some root mappings are context-specific.
func backcompatRootClasses(classes []string, parent *Microformat) []string {
	var rootclasses []string
	var itemClass bool
	for _, class := range classes {
		if c, ok := backcompatRootMap[class]; ok {
			rootclasses = append(rootclasses, c)
		}
		if class == "item" {
			itemClass = true
		}
	}

	// handle implied h-item microformat inside of h-review
	if len(rootclasses) == 0 && parent != nil && itemClass {
		for _, t := range parent.Type {
			if t == "h-review" || t == "h-review-aggregate" {
				rootclasses = append(rootclasses, "h-item")
			}
		}
	}

	return rootclasses
}

// backcompatPropertyClasses returns the v2 property classes for the backcompat
// v1 properties in the provided classes and rel values.  context identifies
// the v2 microformat types (h-card, h-adr, etc) the parsed property belongs to.
func backcompatPropertyClasses(classes []string, rels []string, context []string) []string {
	var classmap = make(map[string]string)
	for _, class := range classes {
		for _, ctx := range context {
			if c, ok := backcompatPropertyMap[ctx][class]; ok {
				parts := strings.SplitN(c, "-", 2)
				classmap[parts[1]] = c
			}
		}
	}
	for _, rel := range rels {
		for _, ctx := range context {
			if c, ok := backcompatRelMap[ctx][rel]; ok {
				parts := strings.SplitN(c, "-", 2)
				classmap[parts[1]] = c
			}
		}
	}

	var propertyclasses []string
	for _, c := range classmap {
		propertyclasses = append(propertyclasses, c)
	}
	return propertyclasses
}

// strip provided URL to its last path segment to serve as a category value.
func backcompatURLCategory(s string) string {
	if s == "" {
		return s
	}
	if p, err := url.Parse(s); err == nil {
		return path.Base(p.Path)
	}
	return s
}

// backcompatIncludeRefs returns references found using the include pattern.
//
// refs includes the IDs of referenced nodes (without any leading '#')
//
// replace is true if the referenced node was identified using a pattern  which
// instructs the referencing node to be replaced entirely, rather than the
// referenced node being amended.
//
// See http://microformats.org/wiki/include-pattern
// See http://microformats.org/wiki/microdata
func (p *parser) backcompatIncludeRefs(node *html.Node) (refs []string, replace bool) {
	classes := getClasses(node)
	for _, class := range classes {
		if class == "include" {
			var id string
			if node.DataAtom == atom.A {
				id = getAttr(node, "href")
			} else if node.DataAtom == atom.Object {
				id = getAttr(node, "data")
			}

			if !strings.HasPrefix(id, "#") {
				// skip links not within the current page
				continue
			}

			id = strings.TrimPrefix(id, "#")
			if id == "" {
				continue
			}

			return append(refs, id), true
		}
	}

	if node.DataAtom == atom.Td {
		refs = append(refs, strings.Fields(getAttr(node, "headers"))...)
	}
	refs = append(refs, strings.Fields(getAttr(node, "itemref"))...)

	return refs, false
}

// backcompatIncludeNode includes referenced notes following the include
// pattern.
//
// see backcompatIncludeRefs for information on refs and replace parameters.
func (p *parser) backcompatIncludeNode(node *html.Node, refs []string, replace bool) *html.Node {
	if len(refs) == 0 {
		return node
	}

	for _, ref := range refs {
		if n := findNodeByID(p.root, ref); n != nil {
			if node != n && !isAncestorNode(node, n) {
				if replace {
					return n
				}
				node.AppendChild(cloneNode(n))
			}
		}
	}

	return node
}

// findNodeByID searches node and its children, returning the node with the
// specified id value.
func findNodeByID(node *html.Node, id string) *html.Node {
	if getAttr(node, "id") == id {
		return node
	}
	for c := node.FirstChild; c != nil; c = c.NextSibling {
		if n := findNodeByID(c, id); n != nil {
			return n
		}
	}
	return nil
}

// isAncestorNode returns true if parent is an ancestor node of child.
func isAncestorNode(child, parent *html.Node) bool {
	for c := child; c != nil; c = c.Parent {
		if c == parent {
			return true
		}
	}
	return false
}

// cloneNode makes a copy of node, detaching any parent or sibling nodes.
func cloneNode(node *html.Node) *html.Node {
	clone := *node
	clone.Parent = nil
	clone.PrevSibling = nil
	clone.NextSibling = nil
	return &clone
}
