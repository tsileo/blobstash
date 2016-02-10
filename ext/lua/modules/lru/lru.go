package lru

import (
	"github.com/hashicorp/golang-lru"
	"github.com/yuin/gopher-lua"
	log "gopkg.in/inconshreveable/log15.v2"
)

type LRUModule struct {
	logger log.Logger
	lru    *lru.Cache
}

func New(logger log.Logger, cache *lru.Cache) *LRUModule {
	return &LRUModule{
		logger: logger,
		lru:    cache,
	}
}

func (lru *LRUModule) Loader(L *lua.LState) int {
	mod := L.SetFuncs(L.NewTable(), map[string]lua.LGFunction{
		"add": lru.add,
		"get": lru.get,
	})
	L.Push(mod)
	return 1
}

func (lru *LRUModule) add(L *lua.LState) int {
	k := L.ToString(1)
	lru.logger.Debug("Added key", "key", k)
	lru.lru.Add(k, L.CheckAny(2))
	return 0
}

func (lru *LRUModule) get(L *lua.LState) int {
	k := L.ToString(1)
	val, ok := lru.lru.Get(k)
	if val == nil {
		val = lua.LNil
	}
	lru.logger.Debug("Get key", "key", k, "val", val, "ok", ok)
	L.Push(val.(lua.LValue))
	L.Push(lua.LBool(ok))
	return 2
}
