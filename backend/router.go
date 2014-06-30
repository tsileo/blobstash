package backend

import (
	"fmt"
	"time"
	"strings"

	"github.com/bitly/go-simplejson"
	"github.com/bitly/go-notify"

	"github.com/tsileo/blobstash/db"
)

func SendDebugData(data string) {
	cmd := fmt.Sprintf("%v: %v", time.Now().UTC().Format(time.RFC3339), data)
	notify.Post("monitor_cmd", cmd)
}

const (
	Read int = iota
	Write
)

// Request is used for Put/Get operations
type Request struct {
	// The following fields are used for routing
	Type int // Whether this is a Put/Read/Exists request (for blob routing only)
	MetaBlob bool // Whether the blob is a meta blob
	Host string
	Archive bool
}

func (req *Request) String() string {
	return fmt.Sprintf("[request type=%v, meta=%v, hostname=%v]", req.Type, req.MetaBlob, req.Host)
}

type BackendAndDB struct {
	Blob BlobHandler
	DB   *db.DB
}

type Router struct {
	Index *db.DB
	Rules []*simplejson.Json
	Backends map[string]BlobHandler
	DBs map[string]*db.DB
	TxManagers map[string]*TxManager
}

func (router *Router) Load() error {
	for _, txm := range router.TxManagers {
		if err := txm.Load(); err != nil {
			return err
		}
	}
	return nil
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

func (router *Router) TxManager(req *Request) *TxManager {
	req.MetaBlob = true
	// Type and Host must be set
	key := router.Route(req)
	txmanager, exists := router.TxManagers[key]
	if !exists {
		panic(fmt.Errorf("backend %v is not registered", key))
	}
	return txmanager
}

func (router *Router) DB(req *Request) *db.DB {
	req.MetaBlob = true
	// Type and Host must be set
	key := router.Route(req)
	db, exists := router.DBs[key]
	if !exists {
		panic(fmt.Errorf("backend %v is not registered", key))
	}
	return db
}

func (router *Router) Put(req *Request, hash string, data []byte) error {
	req.Type = Write
	key := router.Route(req)
	backend, exists := router.Backends[key]
	if !exists {
		panic(fmt.Errorf("backend %v is not registered", key))
	}
	return backend.Put(hash, data)
}

func (router *Router) Exists(req *Request, hash string) bool {
	req.Type = Read
	key := router.Route(req)
	backend, exists := router.Backends[key]
	if !exists {
		panic(fmt.Errorf("backend %v is not registered", key))
	}
	return backend.Exists(hash)
}

func (router *Router) Get(req *Request, hash string) (data []byte, err error) {
	req.Type = Read
	key := router.Route(req)
	backend, exists := router.Backends[key]
	if !exists {
		panic(fmt.Errorf("backend %v is not registered", key))
	}
	return backend.Get(hash)
}

func (router *Router) Enumerate(req *Request, res chan<- string) error {
	req.Type = Read
	key := router.Route(req)
	backend, exists := router.Backends[key]
	if !exists {
		panic(fmt.Errorf("backend %v is not registered", key))
	}
	return backend.Enumerate(res)
}

func (router *Router) Close() {
	for _, backend := range router.Backends {
		backend.Close()
	}
	for _, db := range router.DBs {
		db.Close()
	}
	if router.Index != nil {
		router.Index.Close()
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

func NewRouterFromConfig(json *simplejson.Json, index *db.DB) (*Router, error) {
	rules, err := json.Array()
	if err != nil {
		return nil, fmt.Errorf("failed to parse config, body must be an array")
	}
	rconf := &Router{Rules: []*simplejson.Json{},
		Index: index,
		Backends: make(map[string]BlobHandler),
		DBs: make(map[string]*db.DB),
		TxManagers: make(map[string]*TxManager)}
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
				SendDebugData(fmt.Sprintf("routed blob req %v to %v", req, backend))
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
				SendDebugData(fmt.Sprintf("routed blob req %v to %v", req, backend))
				return backend
			}
		}
	}
	SendDebugData(fmt.Sprintf("router failed to route blob req %v", req))
	return ""
}

// checkRule check if the rule match the given Request
func checkRule(rule string, req *Request) bool {
	switch {
	case rule == "if-meta":
		if req.MetaBlob {
			return true
		}
	case rule == "if-archive":
		if req.Archive {
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
