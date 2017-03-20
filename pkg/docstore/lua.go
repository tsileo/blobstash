package docstore

// package docstore

// TODO(tsileo): use   -tags 'prod' for building binary

import (
	"bytes"
	"fmt"
	"path/filepath"
	"time"

	"github.com/blevesearch/segment"
	log "github.com/inconshreveable/log15"
	"github.com/reiver/go-porterstemmer"
	"github.com/yuin/gopher-lua"

	luautil "a4.io/blobstash/pkg/apps/luautil"
)

type QueryMatcher interface {
	Match(map[string]interface{}) (bool, error)
	Close() error
}

type MatchAllEngine struct{}

func (mae *MatchAllEngine) Match(_ map[string]interface{}) (bool, error) {
	return true, nil
}

func (mae *MatchAllEngine) Close() error { return nil }

type LuaQueryEngine struct {
	storedQueries   map[string]*storedQuery // Stored query store
	storedQueryName string                  // Requested stored query name if any

	code  string
	query interface{} // Raw query
	q     lua.LValue

	matchFunc func(map[string]interface{}) (bool, error)
	L         *lua.LState // Lua state that will live the whole query

	logger log.Logger
}

func (lqe *LuaQueryEngine) Close() error {
	lqe.L.Close()
	return nil
}

func setGlobals(L *lua.LState) {
	// FIXME(tsileo): a `use_index(index_field, value)` and have the optimizer use it
	L.SetGlobal("porterstemmer", L.NewFunction(ltokenize))
	L.SetGlobal("porterstemmer_stem", L.NewFunction(stem))
}

func (docstore *DocStore) newLuaQueryEngine(query *query) (*LuaQueryEngine, error) {
	engine := &LuaQueryEngine{
		storedQueries: docstore.storedQueries,
		query:         query.storedQueryArgs,

		// XXX(tsileo): code expects a Lua script that returns a function(doc) for matching docs
		code: queryToScript(query), // FIXME(tsileo): query.script seems not to work

		storedQueryName: query.storedQuery,
		L:               lua.NewState(),
		q:               lua.LNil,
		logger:          docstore.logger.New("submodule", "lua_query_engine"),
	}
	fmt.Printf("code=\n\n%s\n\n", engine.code)
	setGlobals(engine.L)
	engine.logger.Debug("init", "query", engine.query)
	// Parse the Lua query, which should be defined as a `function(doc) -> bool`, we parse it only once, then we got
	// a "Lua func" Go object which we can call repeatedly for each document.
	// XXX(tsileo): keep the function (along with the Lua context `L` for a few minutes) in a cache, so if a client is paginating
	// through results, it will reuse the func/Lua context. (Cache[hash(script)] = FuncWithContextReadyToCall)
	var ret *lua.LFunction
	if engine.query != nil {
		if engine.storedQueryName != "" {
			// XXX(tsileo): concerns: the script should be checked at startup because right now,
			// a user have to actually try a query before we can see if it's valud Lua.
			engine.logger.Debug("loading stored query", "name", engine.storedQueryName)
			squery, ok := engine.storedQueries[engine.storedQueryName]
			if !ok {
				return nil, fmt.Errorf("Unknown stored query name")
			}
			luautil.AddToPath(engine.L, filepath.Dir(squery.Main))
			engine.L.SetGlobal("query", luautil.InterfaceToLValue(engine.L, engine.query))
			if err := engine.L.DoFile(squery.Main); err != nil {
				panic(err)
			}
			ret = engine.L.Get(-1).(*lua.LFunction)
		}
	} else {
		// XXX(tsileo): queryToString converted the basic function to a script retunring a function
		if err := engine.L.DoString(engine.code); err != nil {
			return nil, err
		}
		ret = engine.L.Get(-1).(*lua.LFunction)
		fmt.Printf("extracted fun %v\n", ret)
	}
	if ret != nil {
		matchDoc := func(doc map[string]interface{}) (bool, error) {
			if err := engine.L.CallByParam(lua.P{
				Fn:      ret,
				NRet:    1,
				Protect: true,
			}, luautil.InterfaceToLValue(engine.L, doc)); err != nil {
				return false, err
			}
			if engine.L.Get(-1) == lua.LTrue {
				return true, nil
			}
			return false, nil
		}

		engine.matchFunc = matchDoc
	}
	return engine, nil
}

func (lqe *LuaQueryEngine) Match(doc map[string]interface{}) (bool, error) {
	start := time.Now()
	var out bool
	L := lqe.L

	if lqe.matchFunc != nil {
		return lqe.matchFunc(doc)
	}

	// FIXME(tsileo): REMOVE ME, DEAD CODE (ensure it is)
	L.SetGlobal("doc", luautil.InterfaceToLValue(L, doc))
	// TODO(tsileo): a debug mode for debug print
	// TODO(tsileo): harvesine function for geoquery
	// TODO(tsileo): handle near:<place>, is:archived, contains:<link|todo|...>, tag:mytag

	// XXX(tsileo): be able to fetch a blob and parse it??
	if err := lqe.L.DoString(lqe.code); err != nil {
		return out, err
	}
	ret := lqe.L.Get(-1)
	if ret == lua.LTrue {
		out = true
	}
	lqe.logger.Debug("match code ran", "duration", time.Since(start))
	lqe.L.Pop(1)
	return out, nil
}

func stem(L *lua.LState) int {
	in := L.ToString(1)
	L.Push(lua.LString(porterstemmer.StemString(in)))
	return 1
}

func ltokenize(L *lua.LState) int {
	in := L.ToString(1)
	out, err := tokenize([]byte(in))
	if err != nil {
		panic(err)
	}
	L.Push(luautil.InterfaceToLValue(L, out))
	return 1
}

func tokenize(data []byte) (map[string]interface{}, error) {
	out := map[string]interface{}{}
	segmenter := segment.NewWordSegmenter(bytes.NewReader(data))
	for segmenter.Segment() {
		if segmenter.Type() == segment.Letter {
			out[porterstemmer.StemString(segmenter.Text())] = true
		}
	}
	if err := segmenter.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
