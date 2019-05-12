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

// Package microformats provides a microformats parser, supporting both v1 and
// v2 syntax.
//
// Usage:
//
//     import "willnorris.com/go/microformats"
//
// Retrive the HTML contents of a page, and call Parse or ParseNode, depending
// on what input you have (an io.Reader or an html.Node).
//
// To parse only a section of an HTML document, use a package like goquery to
// select the root node to parse from.  For example, see cmd/gomf/main.go.
//
// See also: http://microformats.org/wiki/microformats2
package microformats // import "willnorris.com/go/microformats"

import (
	"bytes"
	"fmt"
	"io"
	"net/url"
	"regexp"
	"sort"
	"strings"

	"golang.org/x/net/html"
	"golang.org/x/net/html/atom"
)

var (
	rootClassNames     = regexp.MustCompile(`^h-[a-z\-]+$`)
	propertyClassNames = regexp.MustCompile(`^(p|u|dt|e)-([a-z\-]+)$`)
)

// Microformat specifies a single microformat object and its properties.  It
// may represent a person, an address, a blog post, etc.
type Microformat struct {
	Value      string                   `json:"value,omitempty"`
	HTML       string                   `json:"html,omitempty"`
	Type       []string                 `json:"type"`
	Properties map[string][]interface{} `json:"properties"`
	Shape      string                   `json:"shape,omitempty"`
	Coords     string                   `json:"coords,omitempty"`
	Children   []*Microformat           `json:"children,omitempty"`

	// track whether this microformat has various types of properties or
	// nested microformats. Used in processing implied property values.
	hasNestedMicroformats bool
	hasPProperties        bool
	hasEProperties        bool
	hasUProperties        bool

	// whether this is a v1 microformat parsed in backwards compatible mode
	backcompat bool
}

// Data specifies all of the microformats and data parsed from a single HTML
// page.
type Data struct {
	// Items includes all top-level microformats found on the page.
	Items []*Microformat `json:"items"`

	// Rels includes all related URLs found on the page (<a> or <link>
	// elements with a "rel" value).  Map keys are the rel value, mapped to
	// a slice of URLs with that relation.  For example:
	//
	//     map[string][]string{
	//         "author": {"http://example.com/a", "http://example.com/b"},
	//         "alternate": {"http://example.com/fr"},
	//     }
	//
	// Relative URL values are resolved to absolute URLs using the base URL
	// of the page.
	Rels map[string][]string `json:"rels"`

	// RelURLs maps related URLs found on the page to additional metadata
	// about that relationship. If a URL is linked to more than once, only
	// the metadata for the first link is included here.  Relative URL
	// values are resolved to absolute URLs using the base URL of the page.
	RelURLs map[string]*RelURL `json:"rel-urls"`
}

// RelURL represents the attributes of a URL.  The URL value itself is the map
// key in the RelURLs field of the Data type.
type RelURL struct {
	Rels     []string `json:"rels,omitempty"`
	Text     string   `json:"text,omitempty"`
	Media    string   `json:"media,omitempty"`
	HrefLang string   `json:"hreflang,omitempty"`
	Title    string   `json:"title,omitempty"`
	Type     string   `json:"type,omitempty"`
}

// parser parses a single HTML page for microformats.  parser is not thread
// safe, and should only be used to parse a single document.
type parser struct {
	curData   *Data
	curItem   *Microformat
	base      *url.URL
	baseFound bool

	// root node of the parsed document
	root *html.Node
}

// Parse the microformats found in the HTML document read from r.  baseURL is
// the URL this document was retrieved from and is used to resolve any
// relative URLs.
func Parse(r io.Reader, baseURL *url.URL) *Data {
	doc, _ := html.Parse(r)
	return ParseNode(doc, baseURL)
}

// ParseNode parses the microformats found in doc.  baseURL is the URL this
// document was retrieved from and is used to resolve any relative URLs.
func ParseNode(doc *html.Node, baseURL *url.URL) *Data {
	p := new(parser)
	p.curData = &Data{
		Items:   make([]*Microformat, 0),
		Rels:    make(map[string][]string),
		RelURLs: make(map[string]*RelURL),
	}
	p.base = baseURL
	p.baseFound = false
	p.root = doc
	p.walk(doc)
	return p.curData
}

// expandAttrURLs expands relative URLs in attributes to be absolute URLs.
// Attributes are taken from https://html.spec.whatwg.org/multipage/indices.html#attributes-3.
func (p *parser) expandAttrURLs(node *html.Node) {
	var attr []string
	if isAtom(node, atom.Form) {
		attr = append(attr, "action")
	}
	if isAtom(node, atom.Blockquote, atom.Del, atom.Ins, atom.Q) {
		attr = append(attr, "cite")
	}
	if isAtom(node, atom.Object) {
		attr = append(attr, "data")
	}
	if isAtom(node, atom.Button, atom.Input) {
		attr = append(attr, "formaction")
	}
	if isAtom(node, atom.A, atom.Area, atom.Base, atom.Link) {
		attr = append(attr, "href")
	}
	if isAtom(node, atom.A, atom.Area) {
		attr = append(attr, "ping")
	}
	if isAtom(node, atom.Audio, atom.Embed, atom.Iframe, atom.Img, atom.Input, atom.Script, atom.Source, atom.Track, atom.Video) {
		attr = append(attr, "src")
	}
	if isAtom(node, atom.Video) {
		attr = append(attr, "poster")
	}

	for _, a := range attr {
		value := getAttrPtr(node, a)
		if value != nil {
			*value = expandURL(*value, p.base)
		}
		return
	}

	for c := node.FirstChild; c != nil; c = c.NextSibling {
		p.expandAttrURLs(c)
	}
}

// expandURL expands relative URL r into an absolute URL by resolving it relative to
// base. If r is not a valid URL or base is nil, the original r value is returned.
func expandURL(r string, base *url.URL) string {
	if base != nil {
		if u, err := url.Parse(r); err == nil {
			u = base.ResolveReference(u)
			r = u.String()
		}
	}
	return r
}

// walk the DOM rooted at node, storing parsed microformats in p.
func (p *parser) walk(node *html.Node) {
	var curItem *Microformat
	var priorItem *Microformat
	var rootclasses []string

	classes := getClasses(node)
	for _, class := range classes {
		if rootClassNames.MatchString(class) {
			rootclasses = append(rootclasses, class)
		}

	}

	var backcompat bool
	if len(rootclasses) == 0 {
		rootclasses = backcompatRootClasses(classes, p.curItem)
		if len(rootclasses) > 0 {
			backcompat = true
		}
	}

	if len(rootclasses) > 0 {
		sort.Strings(rootclasses)
		curItem = &Microformat{
			Type:       rootclasses,
			Properties: make(map[string][]interface{}),
			backcompat: backcompat,
		}
		if p.curItem == nil {
			p.curData.Items = append(p.curData.Items, curItem)
		} else {
			p.curItem.hasNestedMicroformats = true
		}
		priorItem = p.curItem
		p.curItem = curItem
	}

	// handle backcompat include pattern
	if p.curItem != nil && p.curItem.backcompat {
		refs, replace := p.backcompatIncludeRefs(node)
		if len(refs) != 0 {
			node = p.backcompatIncludeNode(node, refs, replace)
		}
	}

	if !p.baseFound && isAtom(node, atom.Base) {
		if href := getAttr(node, "href"); href != "" {
			if newbase, err := url.Parse(href); err == nil {
				newbase = p.base.ResolveReference(newbase)
				p.base = newbase
				p.baseFound = true
			}
		}
	}

	var rels []string
	if isAtom(node, atom.A, atom.Link) {
		if rel := getAttr(node, "rel"); rel != "" {
			urlVal := getAttr(node, "href")
			urlVal = expandURL(urlVal, p.base)

			rels = strings.Split(rel, " ")
			for _, relval := range rels {
				var seen bool // whether we've already stored this url for this rel
				for _, u := range p.curData.Rels[relval] {
					if u == urlVal {
						seen = true
					}
				}
				if !seen {
					p.curData.Rels[relval] = append(p.curData.Rels[relval], urlVal)
				}
			}

			if _, ok := p.curData.RelURLs[urlVal]; !ok {
				p.curData.RelURLs[urlVal] = &RelURL{
					Text:     getTextContent(node, nil),
					Rels:     rels,
					Media:    getAttr(node, "media"),
					HrefLang: getAttr(node, "hreflang"),
					Title:    getAttr(node, "title"),
					Type:     getAttr(node, "type"),
				}
			}
		}
	}

	for c := node.FirstChild; c != nil; c = c.NextSibling {
		p.walk(c)
	}

	if curItem != nil {
		// all child elements of node have been processed, and all explicit
		// properties on curItem have been set.

		// Process implied date for 'end' property.
		implyEndDate(curItem)

		if p.curItem == nil || !p.curItem.backcompat {
			// Now process implied property values.
			if _, ok := curItem.Properties["name"]; !ok {
				if !curItem.hasNestedMicroformats && !curItem.hasPProperties && !curItem.hasEProperties {
					name := getImpliedName(node)
					if name != "" {
						curItem.Properties["name"] = append(curItem.Properties["name"], name)
					}
				}
			}
			if _, ok := curItem.Properties["photo"]; !ok {
				photo := getImpliedPhoto(node, p.base)
				if photo != "" {
					curItem.Properties["photo"] = append(curItem.Properties["photo"], photo)
				}
			}
			if _, ok := curItem.Properties["url"]; !ok {
				if !curItem.hasNestedMicroformats && !curItem.hasUProperties {
					url := getImpliedURL(node, p.base)
					if url != "" {
						curItem.Properties["url"] = append(curItem.Properties["url"], url)
					}
				}
			}
		}
		p.curItem = priorItem
	}

	var propertyclasses []string
	if p.curItem != nil && p.curItem.backcompat {
		var itemType []string
		if p.curItem != nil {
			itemType = p.curItem.Type
		}
		propertyclasses = backcompatPropertyClasses(classes, rels, itemType)
	} else {
		for _, class := range classes {
			match := propertyClassNames.FindStringSubmatch(class)
			if match != nil {
				propertyclasses = append(propertyclasses, match[0])
			}
		}
	}
	if len(propertyclasses) > 0 {
		for _, prop := range propertyclasses {
			parts := strings.SplitN(prop, "-", 2)
			prefix, name := parts[0], parts[1]

			var value, embedValue *string
			var htmlbody string
			switch prefix {
			case "p":
				if p.curItem != nil {
					p.curItem.hasPProperties = true
				}
				value = getValueClassPattern(node)
				if value == nil && isAtom(node, atom.Abbr, atom.Link) {
					value = getAttrPtr(node, "title")
				}
				if value == nil && isAtom(node, atom.Data, atom.Input) {
					value = getAttrPtr(node, "value")
				}
				if value == nil && isAtom(node, atom.Img, atom.Area) {
					value = getAttrPtr(node, "alt")
				}
				if value == nil {
					value = new(string)
					*value = strings.TrimSpace(getTextContent(node, p.imageAltSrcValue))
				}
				if curItem != nil && p.curItem != nil {
					embedValue = getFirstPropValue(curItem, "name")
				}
			case "u":
				if p.curItem != nil {
					p.curItem.hasUProperties = true
				}
				if value == nil && isAtom(node, atom.A, atom.Area, atom.Link) {
					value = getAttrPtr(node, "href")
				}
				if value == nil && isAtom(node, atom.Img, atom.Audio, atom.Video, atom.Source) {
					value = getAttrPtr(node, "src")
				}
				if value == nil && isAtom(node, atom.Object) {
					value = getAttrPtr(node, "data")
				}
				if value == nil && isAtom(node, atom.Video) {
					value = getAttrPtr(node, "poster")
				}
				if value != nil {
					*value = expandURL(*value, p.base)
				}
				if value == nil {
					value = getValueClassPattern(node)
				}
				if value == nil && isAtom(node, atom.Abbr) {
					value = getAttrPtr(node, "title")
				}
				if value == nil && isAtom(node, atom.Data, atom.Input) {
					value = getAttrPtr(node, "value")
				}
				if value == nil {
					value = new(string)
					*value = strings.TrimSpace(getTextContent(node, nil))
				}
				if curItem != nil && p.curItem != nil {
					embedValue = getFirstPropValue(curItem, "url")
				}

				// for category URLs in backcompat mode, strip to the last path segment
				if p.curItem != nil && p.curItem.backcompat && name == "category" {
					*value = backcompatURLCategory(*value)
				}
			case "e":
				if p.curItem != nil {
					p.curItem.hasEProperties = true
				}
				value = new(string)
				*value = strings.TrimSpace(getTextContent(node, p.imageAltSrcValue))
				var buf bytes.Buffer

				for c := node.FirstChild; c != nil; c = c.NextSibling {
					p.expandAttrURLs(c) // microformats/microformats2-parsing#38
					html.Render(&buf, c)
				}
				htmlbody = strings.TrimSpace(buf.String())

				// HTML spec: Serializing HTML Fragments algorithm does not include
				// a trailing slash, so remove it.  Nor should apostrophes be
				// encoded, which golang.org/x/net/html is doing.
				htmlbody = strings.Replace(htmlbody, `/>`, `>`, -1)
				htmlbody = strings.Replace(htmlbody, `&#39;`, `'`, -1)
			case "dt":
				if value == nil {
					value = getDateTimeValue(node)
				}
				if value == nil && isAtom(node, atom.Time, atom.Ins, atom.Del) {
					value = getAttrPtr(node, "datetime")
				}
				if value == nil && isAtom(node, atom.Abbr) {
					value = getAttrPtr(node, "title")
				}
				if value == nil && isAtom(node, atom.Data, atom.Input) {
					value = getAttrPtr(node, "value")
				}
				if value == nil {
					value = new(string)
					*value = strings.TrimSpace(getTextContent(node, nil))
				}
			}
			if curItem != nil && p.curItem != nil {
				if embedValue == nil {
					embedValue = value
				}
				p.curItem.Properties[name] = append(p.curItem.Properties[name], &Microformat{
					Type:       curItem.Type,
					Properties: curItem.Properties,
					Coords:     curItem.Coords,
					Shape:      curItem.Shape,
					Value:      *embedValue,
					HTML:       htmlbody,
				})
			} else if value != nil && p.curItem != nil {
				if htmlbody != "" {
					p.curItem.Properties[name] = append(p.curItem.Properties[name], map[string]interface{}{"value": *value, "html": htmlbody})
				} else {
					p.curItem.Properties[name] = append(p.curItem.Properties[name], *value)
				}
			}
		}
	} else {
		if curItem != nil && p.curItem != nil {
			p.curItem.Children = append(p.curItem.Children, curItem)
			p.curItem.hasNestedMicroformats = true
		}
	}
}

// getClasses returns all of the classes on node.
func getClasses(node *html.Node) []string {
	if c := getAttrPtr(node, "class"); c != nil {
		return strings.Split(*c, " ")
	}
	return nil
}

// hasMatchingClass whether node contains a class that matches regex.
func hasMatchingClass(node *html.Node, regex *regexp.Regexp) bool {
	classes := getClasses(node)
	for _, class := range classes {
		if regex.MatchString(class) {
			return true
		}
	}
	return false
}

// getAttr returns the value of the specified attribute on node.
func getAttr(node *html.Node, name string) string {
	if v := getAttrPtr(node, name); v != nil {
		return *v
	}
	return ""
}

// getAttr returns pointer to value of the specified attribute on node.  If
// node does not contain the specified attribute, nil will be returned.
func getAttrPtr(node *html.Node, name string) *string {
	if node == nil {
		return nil
	}
	for i, attr := range node.Attr {
		if strings.EqualFold(attr.Key, name) {
			return &node.Attr[i].Val
		}
	}
	return nil
}

// isAtom returns whether node's atom is one of atoms.
func isAtom(node *html.Node, atoms ...atom.Atom) bool {
	if node == nil {
		return false
	}
	for _, atom := range atoms {
		if atom == node.DataAtom {
			return true
		}
	}
	return false
}

// getTextContent returns the text content of node, following the common
// microformats v2 algorithm.  Nested script and style elements are ignored,
// and img elements are run through imgFn.  If imgFn is nil, img elements are
// ignored as well.
func getTextContent(node *html.Node, imgFn func(*html.Node) string) string {
	if node == nil {
		return ""
	}
	if isAtom(node, atom.Script, atom.Style) {
		return ""
	}
	if isAtom(node, atom.Img) && imgFn != nil {
		return imgFn(node)
	}
	if node.Type == html.TextNode {
		return node.Data
	}
	var buf bytes.Buffer
	for c := node.FirstChild; c != nil; c = c.NextSibling {
		buf.WriteString(getTextContent(c, imgFn))
	}
	return buf.String()
}

// imageAltValue returns the value of node's alt attribute.
func imageAltValue(node *html.Node) string {
	return getAttr(node, "alt")
}

// imageAltSrcValue returns the value of node's alt attribute.  If node doesn't
// have an alt attribute, the value of node's src attribute is expanded to an
// absolute URL and returned.
func (p *parser) imageAltSrcValue(node *html.Node) string {
	if v := getAttrPtr(node, "alt"); v != nil {
		return *v
	}
	if v := getAttrPtr(node, "src"); v != nil {
		return fmt.Sprintf(" %v ", expandURL(*v, p.base))
	}
	return ""
}

// getOnlyChild returns the sole child of node.  Returns nil if node has zero
// or more than one child.
func getOnlyChild(node *html.Node) *html.Node {
	if node == nil {
		return nil
	}
	var n *html.Node
	for c := node.FirstChild; c != nil; c = c.NextSibling {
		if c.Type == html.ElementNode {
			if n == nil {
				n = c
			} else {
				return nil
			}
		}
	}
	return n
}

// getOnlyChild returns the sole child of node with the specified atom.
// Returns nil if node has zero or more than one child with that atom.
func getOnlyChildAtom(node *html.Node, atom atom.Atom) *html.Node {
	if node == nil {
		return nil
	}
	var n *html.Node
	for c := node.FirstChild; c != nil; c = c.NextSibling {
		if c.Type == html.ElementNode && c.DataAtom == atom {
			if n == nil {
				n = c
			} else {
				return nil
			}
		}
	}
	return n
}

// getOnlyChild returns the sole child of node with the specified atom and
// attribute.  Returns nil if node has zero or more than one child with that
// atom and attribute.
func getOnlyChildAtomWithAttr(node *html.Node, atom atom.Atom, attr string) *html.Node {
	if node == nil {
		return nil
	}
	var n *html.Node
	for c := node.FirstChild; c != nil; c = c.NextSibling {
		if c.Type == html.ElementNode && c.DataAtom == atom && getAttrPtr(c, attr) != nil {
			if n == nil {
				n = c
			} else {
				return nil
			}
		}
	}
	return n
}

// getImpliedName gets the implied name value for node.
//
// See http://microformats.org/wiki/microformats2-parsing
func getImpliedName(node *html.Node) string {
	var name *string
	if isAtom(node, atom.Img, atom.Area) {
		name = getAttrPtr(node, "alt")
	}
	if name == nil && isAtom(node, atom.Abbr) {
		name = getAttrPtr(node, "title")
	}

	if name == nil {
		subnode := getOnlyChild(node)
		if subnode != nil && subnode.DataAtom == atom.Img && !hasMatchingClass(subnode, rootClassNames) {
			name = getAttrPtr(subnode, "alt")
		}
	}
	if name == nil {
		subnode := getOnlyChild(node)
		if subnode != nil && subnode.DataAtom == atom.Area && !hasMatchingClass(subnode, rootClassNames) {
			name = getAttrPtr(subnode, "alt")
		}
	}
	if name == nil {
		subnode := getOnlyChild(node)
		if subnode != nil && subnode.DataAtom == atom.Abbr && !hasMatchingClass(subnode, rootClassNames) {
			name = getAttrPtr(subnode, "title")
		}
	}

	if name == nil {
		subnode := getOnlyChild(node)
		if subnode != nil && !hasMatchingClass(subnode, rootClassNames) {
			subsubnode := getOnlyChild(subnode)
			if subsubnode != nil && subsubnode.DataAtom == atom.Img && !hasMatchingClass(subsubnode, rootClassNames) {
				name = getAttrPtr(subsubnode, "alt")
			}
		}
	}
	if name == nil {
		subnode := getOnlyChild(node)
		if subnode != nil && !hasMatchingClass(subnode, rootClassNames) {
			subsubnode := getOnlyChild(subnode)
			if subsubnode != nil && subsubnode.DataAtom == atom.Area && !hasMatchingClass(subsubnode, rootClassNames) {
				name = getAttrPtr(subsubnode, "alt")
			}
		}
	}
	if name == nil {
		subnode := getOnlyChild(node)
		if subnode != nil && !hasMatchingClass(subnode, rootClassNames) {
			subsubnode := getOnlyChild(subnode)
			if subsubnode != nil && subsubnode.DataAtom == atom.Abbr {
				name = getAttrPtr(subsubnode, "title")
			}
		}
	}

	if name == nil {
		name = new(string)
		*name = strings.TrimSpace(getTextContent(node, imageAltValue))
	}
	return strings.TrimSpace(*name)
}

// getImpliedName gets the implied photo value for node.
//
// See http://microformats.org/wiki/microformats2-parsing
func getImpliedPhoto(node *html.Node, baseURL *url.URL) string {
	var photo *string
	if photo == nil && isAtom(node, atom.Img) {
		photo = getAttrPtr(node, "src")
	}
	if photo == nil && isAtom(node, atom.Object) {
		photo = getAttrPtr(node, "data")
	}

	if photo == nil {
		subnode := getOnlyChildAtomWithAttr(node, atom.Img, "src")
		if subnode != nil && !hasMatchingClass(subnode, rootClassNames) {
			photo = getAttrPtr(subnode, "src")
		}
	}
	if photo == nil {
		subnode := getOnlyChildAtomWithAttr(node, atom.Object, "data")
		if subnode != nil && !hasMatchingClass(subnode, rootClassNames) {
			photo = getAttrPtr(subnode, "data")
		}
	}

	if photo == nil {
		subnode := getOnlyChild(node)
		if subnode != nil && !hasMatchingClass(subnode, rootClassNames) {
			subsubnode := getOnlyChildAtomWithAttr(subnode, atom.Img, "src")
			if subsubnode != nil && !hasMatchingClass(subsubnode, rootClassNames) {
				photo = getAttrPtr(subsubnode, "src")
			}
		}
	}
	if photo == nil {
		subnode := getOnlyChild(node)
		if subnode != nil && !hasMatchingClass(subnode, rootClassNames) {
			subsubnode := getOnlyChildAtomWithAttr(subnode, atom.Object, "data")
			if subsubnode != nil && !hasMatchingClass(subsubnode, rootClassNames) {
				photo = getAttrPtr(subsubnode, "data")
			}
		}
	}

	if photo == nil {
		return ""
	}
	return expandURL(*photo, baseURL)
}

// getImpliedName gets the implied url value for node.
//
// See http://microformats.org/wiki/microformats2-parsing
func getImpliedURL(node *html.Node, baseURL *url.URL) string {
	var value *string
	if value == nil && isAtom(node, atom.A, atom.Area) {
		value = getAttrPtr(node, "href")
	}

	if value == nil {
		subnode := getOnlyChildAtomWithAttr(node, atom.A, "href")
		if subnode != nil && !hasMatchingClass(subnode, rootClassNames) {
			value = getAttrPtr(subnode, "href")
		}
	}
	if value == nil {
		subnode := getOnlyChildAtomWithAttr(node, atom.Area, "href")
		if subnode != nil && !hasMatchingClass(subnode, rootClassNames) {
			value = getAttrPtr(subnode, "href")
		}
	}

	if value == nil {
		subnode := getOnlyChild(node)
		if subnode != nil && !hasMatchingClass(subnode, rootClassNames) {
			subsubnode := getOnlyChildAtomWithAttr(subnode, atom.A, "href")
			if subsubnode != nil && !hasMatchingClass(subsubnode, rootClassNames) {
				value = getAttrPtr(subsubnode, "href")
			}
		}
	}
	if value == nil {
		subnode := getOnlyChild(node)
		if subnode != nil && !hasMatchingClass(subnode, rootClassNames) {
			subsubnode := getOnlyChildAtomWithAttr(subnode, atom.Area, "href")
			if subsubnode != nil && !hasMatchingClass(subsubnode, rootClassNames) {
				value = getAttrPtr(subsubnode, "href")
			}
		}
	}

	if value == nil {
		return ""
	}
	return expandURL(*value, baseURL)
}

// getValueClassPattern gets the value of node using the value class pattern.
//
// See http://microformats.org/wiki/value-class-pattern
func getValueClassPattern(node *html.Node) *string {
	values := parseValueClassPattern(node, false)
	if len(values) > 0 {
		val := strings.Join(values, "")
		return &val
	}
	return nil
}

// parseValueClassPattern parses node for values using the value class pattern.
// If dt is true, the rules for date and time parsing will be used.
func parseValueClassPattern(node *html.Node, dt bool) []string {
	if node == nil {
		return nil
	}
	var values []string
	for c := node.FirstChild; c != nil; c = c.NextSibling {
		classes := getClasses(c)
		var valueClass, valueTitleClass bool
		for _, class := range classes {
			if class == "value" {
				valueClass = true
			}
			if class == "value-title" {
				valueTitleClass = true
			}
		}
		if valueTitleClass {
			values = append(values, *getAttrPtr(c, "title"))
		} else if valueClass {
			if isAtom(c, atom.Img, atom.Area) && getAttrPtr(c, "alt") != nil {
				values = append(values, *getAttrPtr(c, "alt"))
			} else if isAtom(c, atom.Data) && getAttrPtr(c, "value") != nil {
				values = append(values, *getAttrPtr(c, "value"))
			} else if isAtom(c, atom.Abbr) && getAttrPtr(c, "title") != nil {
				values = append(values, *getAttrPtr(c, "title"))
			} else if dt && isAtom(c, atom.Del, atom.Ins, atom.Time) && getAttrPtr(c, "datetime") != nil {
				values = append(values, *getAttrPtr(c, "datetime"))
			} else {
				values = append(values, strings.TrimSpace(getTextContent(c, nil)))
			}
		}
	}

	return values
}

// getFirstPropValue returns the first property value for prop in item.
func getFirstPropValue(item *Microformat, prop string) *string {
	values := item.Properties[prop]
	if len(values) > 0 {
		if v, ok := values[0].(string); ok {
			return &v
		}
	}
	return nil
}
