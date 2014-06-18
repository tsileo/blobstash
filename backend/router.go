package backend

import (
	"fmt"
	"strings"

	"github.com/bitly/go-simplejson"
)

const (
	Put byte = iota
	Read
	Exists
)

// Request is used for Put/Get operations
type Request struct {
	Hash string
	Blob []byte
	Error error // Store the error if any
	Exist bool

	// The following fields are used for routing
	Type byte // Whether this is a Put/Read/Exists request
	MetaBlob bool // Whether the blob is a meta blob
	Hostname string
}

func (req *Request) String() string {
	return fmt.Sprintf("[request type=%v, meta=%v, hostname=%v]", req.Type, req.MetaBlob, req.Hostname)
}

func NewReadRequest(hostname, hash string, metaBlob bool) *Request {
	return &Request{Hostname: hostname,
		Hash: hash,
		Type: Read,
		MetaBlob :metaBlob}
}

type Router struct {
	Rules []*simplejson.Json
}

func NewRouterFromConfig(body []byte) (*Router, error) {
	json, err := simplejson.NewJson(body)
	if err != nil {
		return nil, err
	}
	rules, err := json.Array()
	if err != nil {
		return nil, fmt.Errorf("failed to parse config, body must be an array")
	}
	rconf := &Router{Rules: []*simplejson.Json{}}
	for i, _ := range rules {
		rconf.Rules = append(rconf.Rules, json.GetIndex(i))
	}
	return rconf, nil
}

// Route the request and return the backend key that match the request
func (router *Router) Route(req *Request) string {
	for _, baseRule := range router.Rules {
		basicRule, err := baseRule.Array()
		_, basicMode := basicRule[0].(string)
		if err == nil && basicMode {
			// Basic rule handling [conf, backend]
			backend := basicRule[1].(string)
			rule := basicRule[0].(string)
			if checkRule(rule, req) {
				return backend
			}
		} else {
			backend := baseRule.GetIndex(1).MustString()
			subRules, err := baseRule.GetIndex(0).StringArray()
			if err != nil {
				panic(fmt.Errorf("bad rule %v", baseRule.GetIndex(0)))
			}
			match := true
			for _, rule := range subRules {
				if !checkRule(rule, req) && match {
					match = false
				}
			}
			if match {
				return backend
			}
		}
	}
	return ""
}

func checkRule(rule string, req *Request) bool {
	switch {
	case rule == "if-meta":
		if req.MetaBlob {
			return true
		}
	case strings.HasPrefix(rule, "if-host-"):
		host := strings.Replace(rule, "if-host-", "", 1)
		if strings.ToLower(req.Hostname) == strings.ToLower(host) {
			return true
		}
	case rule == "default":
		return true
	default:
		panic(fmt.Errorf("failed to parse rule \"%v\"", rule))
	}
	return false
}

type RequestRouter struct {
	router *Router
	backends map[string]BlobHandler
}

func (router *RequestRouter) Execute(req *Request) {
	switch {
	case req.Type == Put:

	case req.Type == Read:

	case req.Type == Exists:

	default:
		panic(fmt.Errorf("unknown req type %v", req.Type))
	}

}
