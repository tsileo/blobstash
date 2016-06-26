/*

package router implements a router to forward blob to the right backend.

*/

package router

import (
	"fmt"
	"strings"

	"github.com/tsileo/blobstash/pkg/ctxutil"
	"golang.org/x/net/context"

	"github.com/tsileo/blobstash/pkg/backend"
)

type Rule struct {
	Conds   []string
	Backend string
}

func decodeRules(trules []interface{}) []*Rule {
	rules := []*Rule{}
	for _, root := range trules {
		r, _ := root.([]interface{})
		var rule *Rule
		rule = &Rule{
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
func (router *Router) Route(ctx context.Context) backend.BlobHandler {
	return router.Backends[router.route(ctx)]
}

// Route the request and return the backend key that match the request
func (router *Router) route(ctx context.Context) string {
	for _, rule := range router.Rules {
		match := true
		for _, cond := range rule.Conds {
			match = match && checkRule(ctx, cond)
		}
		if match {
			return rule.Backend
		}
	}
	return ""
}

// checkRule checks if the rule match the given Request
func checkRule(ctx context.Context, rule string) bool {
	switch {
	case rule == "default":
		return true
	case strings.HasPrefix(rule, "if-ns-"):
		ns := strings.Replace(rule, "if-ns-", "", 1)
		var currentNs string
		if ns, ok := ctxutil.Namespace(ctx); ok {
			currentNs = ns
		}
		if strings.ToLower(currentNs) == strings.ToLower(ns) {
			return true
		}
	default:
		panic(fmt.Errorf("failed to parse rule \"%v\"", rule))
	}
	return false
}

// ResolveBackends constructs the list of needed backend key
// by inspecting the rules
func (router *Router) ResolveBackends() []string {
	res := []string{}
	for _, r := range router.Rules {
		res = append(res, r.Backend)
	}
	return res
}
