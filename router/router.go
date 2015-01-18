/*

package router implements a router to forward blob read/write request to backends.

*/

package router

import (
	"fmt"
	"strings"

	"github.com/tsileo/blobstash/backend"
)

const (
	Read int = iota
	Write
)

// Request is used for Put/Get operations
type Request struct {
	// The following fields are used for routing
	Type      int  // Whether this is a Put/Read/Exists request (for blob routing only)
	MetaBlob  bool // Whether the blob is a meta blob
	Namespace string
}

type Rule struct {
	Conds   []string
	Backend string
}

func decodeRules(trules []interface{}) []*Rule {
	rules := []*Rule{}
	for _, root := range trules {
		r := root.([]interface{})
		rule := &Rule{
			Backend: r[1].(string),
			Conds:   []string{},
		}
		conds, check := r[0].([]interface{})
		if check {
			for _, cond := range conds {
				rule.Conds = append(rule.Conds, cond.(string))
			}
		} else {
			rule.Conds = append(rule.Conds, r[0].(string))
		}
		rules = append(rules, rule)
	}
	return rules
}

func (req *Request) String() string {
	return fmt.Sprintf("[request type=%v, meta=%v, ns=%v]",
		req.Type, req.MetaBlob, req.Namespace)
}
func (req *Request) Meta() *Request {
	return &Request{
		Type:      req.Type,
		MetaBlob:  true,
		Namespace: req.Namespace,
	}
}

type Router struct {
	Backends map[string]backend.BlobHandler
	Rules    []*Rule
}

func New(trules []interface{}) *Router {
	rules := decodeRules(trules)
	return &Router{
		Backends: map[string]backend.BlobHandler{},
		Rules:    rules,
	}
}

// Route the request and return the backend key that match the request
func (router *Router) Route(req *Request) string {
	for _, rule := range router.Rules {
		match := true
		for _, cond := range rule.Conds {
			match = match && checkRule(cond, req)
		}
		if match {
			return rule.Backend
		}
	}
	return ""
}

// checkRule check if the rule match the given Request
func checkRule(rule string, req *Request) bool {
	switch {
	case rule == "default":
		return true
	case rule == "if-meta":
		if req.MetaBlob {
			return true
		}
	case strings.HasPrefix(rule, "if-ns-"):
		ns := strings.Replace(rule, "if-ns-", "", 1)
		if strings.ToLower(req.Namespace) == strings.ToLower(ns) {
			return true
		}
	default:
		panic(fmt.Errorf("failed to parse rule \"%v\"", rule))
	}
	return false
}
