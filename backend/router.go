package backend

import (
	"fmt"
	"strings"

	"github.com/bitly/go-simplejson"
)

const (
	Read byte = iota
	Write
)

// Request is used for Put/Get operations
type Request struct {
	// The following fields are used for routing
	Type byte // Whether this is a Put/Read/Exists request
	MetaBlob bool // Whether the blob is a meta blob
	Host string
}

func (req *Request) String() string {
	return fmt.Sprintf("[request type=%v, meta=%v, hostname=%v]", req.Type, req.MetaBlob, req.Host)
}

type Router struct {
	Rules []*simplejson.Json

	Host string // Host used for rules checking

	Backends map[string]BlobHandler
}

// ResolveBackends construct the list of needed backend key
// by inspecting the rules
func (router *Router) ResolveBackends() []string {
	backends := []string{}
	for _, baseRule := range router.Rules {
		basicRule, err := baseRule.Array()
		_, basicMode := basicRule[0].(string)
		if err == nil && basicMode {
			// Basic rule handling [conf, backend]
			backends = append(backends, basicRule[1].(string))
		} else {
			backends = append(backends, baseRule.GetIndex(1).MustString())
		}
	}
	return backends
}

// TODO a way to set host

func (router *Router) put(metaBlob bool, hash string, data []byte) error {
	req := &Request{Host: router.Host,
		Type: Write,
		MetaBlob: metaBlob}
	key := router.Route(req)
	backend, exists := router.Backends[key]
	if !exists {
		panic(fmt.Errorf("backend %v is not registered", key))
	}
	return backend.Put(hash, data)
}

func (router *Router) Put(hash string, data []byte) error {
	return router.put(false, hash, data)
}

func (router *Router) MetaPut(hash string, data []byte) error {
	return router.put(true, hash, data)
}

func (router *Router) exists(metaBlob bool, hash string) bool {
	req := &Request{Host: router.Host,
		Type: Read,
		MetaBlob: metaBlob}
	key := router.Route(req)
	backend, exists := router.Backends[key]
	if !exists {
		panic(fmt.Errorf("backend %v is not registered", key))
	}
	return backend.Exists(hash)
}

func (router *Router) Exists(hash string) bool {
	return router.exists(false, hash)
}

func (router *Router) MetaExists(hash string) bool {
	return router.exists(true, hash)
}

func (router *Router) get(metaBlob bool, hash string) (data []byte, err error) {
	req := &Request{Host: router.Host,
		Type: Read,
		MetaBlob: metaBlob}
	key := router.Route(req)
	backend, exists := router.Backends[key]
	if !exists {
		panic(fmt.Errorf("backend %v is not registered", key))
	}
	return backend.Get(hash)
}

func (router *Router) Get(hash string) (data []byte, err error) {
	return router.get(false, hash)
}

func (router *Router) MetaGet(hash string) (data []byte, err error) {
	return router.get(true, hash)
}

func (router *Router) enumerate(metaBlob bool, res chan<- string) error {
	req := &Request{Host: router.Host,
		Type: Read,
		MetaBlob: metaBlob}
	key := router.Route(req)
	backend, exists := router.Backends[key]
	if !exists {
		panic(fmt.Errorf("backend %v is not registered", key))
	}
	return backend.Enumerate(res)
}

func (router *Router) Enumerate(res chan<- string) error {
	return router.enumerate(false, res)
}

func (router *Router) MetaEnumerate(res chan<- string) error {
	return router.enumerate(true, res)
}

func (router *Router) Close() {
	for _, backend := range router.Backends {
		backend.Close()
	}
}

func (router *Router) Done() error {
	for _, backend := range router.Backends {
		if err := backend.Done(); err != nil {
			return err
		}
	}
	return nil
}

func NewRouterFromConfig(json *simplejson.Json) (*Router, error) {
	rules, err := json.Array()
	if err != nil {
		return nil, fmt.Errorf("failed to parse config, body must be an array")
	}
	rconf := &Router{Rules: []*simplejson.Json{}, Backends: make(map[string]BlobHandler)}
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

// checkRule check if the rule match the given Request
func checkRule(rule string, req *Request) bool {
	switch {
	case rule == "if-meta":
		if req.MetaBlob {
			return true
		}
	case strings.HasPrefix(rule, "if-host-"):
		host := strings.Replace(rule, "if-host-", "", 1)
		if strings.ToLower(req.Host) == strings.ToLower(host) {
			return true
		}
	case rule == "default":
		return true
	default:
		panic(fmt.Errorf("failed to parse rule \"%v\"", rule))
	}
	return false
}
