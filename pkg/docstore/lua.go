package docstore

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/blevesearch/segment"
	"github.com/dchest/blake2b"
	log "github.com/inconshreveable/log15"
	"github.com/reiver/go-porterstemmer"
	"github.com/yuin/gopher-lua"

	luautil "a4.io/blobstash/pkg/apps/luautil"
	"a4.io/blobstash/pkg/config"
	"a4.io/blobstash/pkg/filetree"
	filetreeLua "a4.io/blobstash/pkg/filetree/lua"
	"a4.io/blobstash/pkg/stash/store"
	"a4.io/gluapp/util"
	"a4.io/gluarequire2"
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

type LuaHook struct {
	L        *lua.LState // A pointer of the state from `LuaHooks`
	hookFunc *lua.LFunction
	ID       string
}

func NewLuaHook(L *lua.LState, code string) (*LuaHook, error) {
	if err := L.DoString(code); err != nil {
		return nil, err
	}
	hookFunc := L.Get(-1).(*lua.LFunction)
	L.Pop(1)
	return &LuaHook{
		L:        L,
		hookFunc: hookFunc,
		ID:       fmt.Sprintf("%x", blake2b.Sum256([]byte(code))),
	}, nil
}

// TODO(tsileo): helper for validation like for required fields and returns details for 422 error (field error details)
func (h *LuaHook) Execute(doc map[string]interface{}) (map[string]interface{}, error) {
	if err := h.L.CallByParam(lua.P{
		Fn:      h.hookFunc,
		NRet:    1,
		Protect: true,
	}, luautil.InterfaceToLValue(h.L, doc)); err != nil {
		fmt.Printf("failed to call pre put hook func: %+v %+v\n", doc, err)
		return nil, err
	}
	newDoc := luautil.TableToMap(h.L.Get(-1).(*lua.LTable))
	h.L.Pop(1)
	return newDoc, nil
}

func (h *LuaHook) ExecuteNoResult(doc map[string]interface{}) error {
	if err := h.L.CallByParam(lua.P{
		Fn:      h.hookFunc,
		NRet:    0,
		Protect: true,
	}, luautil.InterfaceToLValue(h.L, doc)); err != nil {
		fmt.Printf("failed to call pre put hook func: %+v %+v\n", doc, err)
		return err
	}
	return nil
}

func (h *LuaHook) ExecuteReduce(key string, docs []map[string]interface{}) (map[string]interface{}, error) {
	if err := h.L.CallByParam(lua.P{
		Fn:      h.hookFunc,
		NRet:    1,
		Protect: true,
	}, lua.LString(key), luautil.InterfaceToLValue(h.L, docs)); err != nil {
		fmt.Printf("failed to call pre put hook func: %+v %+v\n", docs, err)
		return nil, err
	}
	newDoc := luautil.TableToMap(h.L.Get(-1).(*lua.LTable))
	h.L.Pop(1)
	return newDoc, nil
}

type MapReduceEngine struct {
	L *lua.LState

	M *LuaHook // Map
	R *LuaHook // Reduce
	// F *LuaHook // Finalize, not useful now as reduce is only called once per key

	mapCode, reduceCode string

	reduced bool

	emitted map[string][]map[string]interface{}

	sync.Mutex
}

func (mre *MapReduceEngine) Map(doc map[string]interface{}) error {
	if mre.M == nil {
		return fmt.Errorf("Map hook no set")
	}
	mre.Lock()
	defer mre.Unlock()
	if mre.reduced {
		return fmt.Errorf("already reduced")
	}
	if err := mre.M.ExecuteNoResult(doc); err != nil {
		return err
	}
	return nil
}

func (mre *MapReduceEngine) reduce() error {
	if mre.R == nil {
		return fmt.Errorf("Reduce hook no set")
	}
	for key, values := range mre.emitted {
		newValues, err := mre.R.ExecuteReduce(key, values)
		if err != nil {
			return err
		}
		mre.emitted[key] = []map[string]interface{}{newValues}
	}
	mre.reduced = true
	return nil
}

func (mre *MapReduceEngine) Reduce(other *MapReduceEngine) error {
	if mre.R == nil {
		return fmt.Errorf("Reduce hook no set")
	}
	mre.Lock()
	defer mre.Unlock()
	if !mre.reduced {
		if err := mre.reduce(); err != nil {
			return err
		}
	}

	if other != nil {
		if !other.reduced {
			if err := other.reduce(); err != nil {
				return err
			}
		}
		for k, vs := range other.emitted {
			if cvs, ok := mre.emitted[k]; ok {
				newValues, err := mre.R.ExecuteReduce(k, append(cvs, vs...))
				if err != nil {
					return err
				}
				mre.emitted[k] = []map[string]interface{}{newValues}
			}
		}
	}

	return nil
}

func (mre *MapReduceEngine) Finalize() (map[string]map[string]interface{}, error) {
	// TOOD(tsileo): support finalize
	if !mre.reduced {
		return nil, fmt.Errorf("must reduce first")
	}
	out := map[string]map[string]interface{}{}
	for k, values := range mre.emitted {
		if len(values) > 1 {
			return nil, fmt.Errorf("expected only 1 value per key, got %d", len(values))
		}
		out[k] = values[0]
	}
	return out, nil
}

func (mre *MapReduceEngine) Close() { mre.L.Close() }

func (mre *MapReduceEngine) emit(L *lua.LState) int {
	key := L.ToString(1)
	value := luautil.TableToMap(L.ToTable(2))
	if _, ok := mre.emitted[key]; ok {
		mre.emitted[key] = append(mre.emitted[key], value)
	} else {
		mre.emitted[key] = []map[string]interface{}{value}
	}
	return 0
}

// SetupMap loads the map function (as a string, the code must return a function)
func (mre *MapReduceEngine) SetupMap(code string) error {
	hook, err := NewLuaHook(mre.L, code)
	if err != nil {
		return err
	}
	mre.mapCode = code
	mre.M = hook
	return nil
}

// SetupReduce loads the reduce function (as a string, the code must return a function)
func (mre *MapReduceEngine) SetupReduce(code string) error {
	hook, err := NewLuaHook(mre.L, code)
	if err != nil {
		return err
	}
	mre.reduceCode = code
	mre.R = hook
	return nil
}

// Duplicate returns a new `MapReduceEngine` with the same map and reduce hook as the current instance.
func (mre *MapReduceEngine) Duplicate() (*MapReduceEngine, error) {
	n := NewMapReduceEngine()
	if mre.mapCode == "" || mre.reduceCode == "" {
		return nil, fmt.Errorf("a map reduce engine must be configured before duplication")
	}
	if err := n.SetupMap(mre.mapCode); err != nil {
		return nil, err
	}
	if err := n.SetupReduce(mre.reduceCode); err != nil {
		return nil, err
	}
	return n, nil
}

func NewMapReduceEngine() *MapReduceEngine {
	state := lua.NewState()
	mre := &MapReduceEngine{
		L:       state,
		emitted: map[string][]map[string]interface{}{},
	}
	state.SetGlobal("emit", state.NewFunction(mre.emit))
	return mre
}

type LuaHooks struct {
	hooks  map[string]map[string]*LuaHook
	L      *lua.LState
	config *config.Config
	sync.Mutex
}

func setupCmd(cwd string) func(*lua.LState) int {
	return func(L *lua.LState) int {
		// register functions to the table
		mod := L.SetFuncs(L.NewTable(), map[string]lua.LGFunction{
			"run": func(L *lua.LState) int {
				parts := strings.Split(L.ToString(1), " ")
				cmd := exec.Command(parts[0], parts[1:]...)
				cmd.Dir = cwd
				err := cmd.Run()
				var out string
				if err != nil {
					out = err.Error()
				}
				L.Push(lua.LString(out))
				return 1
			},
		})
		// returns the module
		L.Push(mod)
		return 1
	}
}

func newLuaHooks(conf *config.Config, ft *filetree.FileTree, bs store.BlobStore) (*LuaHooks, error) {
	hooks := &LuaHooks{
		config: conf,
		L:      lua.NewState(),
		hooks:  map[string]map[string]*LuaHook{},
	}

	// Load the "filetree" module
	filetreeLua.Setup(hooks.L, ft, bs)
	// FIXME(tsileo): better CWD
	util.Setup(hooks.L, "/tmp")
	if c := conf.Docstore; c != nil {
		if ch := c.Hooks; ch != nil {
			for col, ops := range ch {
				for op, path := range ops {
					data, err := ioutil.ReadFile(path)
					if err != nil {
						return nil, err
					}
					if err := hooks.Register(col, op, string(data)); err != nil {
						return nil, err
					}
				}
			}
		}
	}

	return hooks, nil
}

func (lh *LuaHooks) Register(col, op, code string) error {
	lh.Lock()
	defer lh.Unlock()
	ops, ok := lh.hooks[col]
	if !ok {
		lh.hooks[col] = map[string]*LuaHook{}
		ops = lh.hooks[col]
	}
	h, err := NewLuaHook(lh.L, code)
	if err != nil {
		return err
	}
	ops[op] = h
	fmt.Printf("RESGISTERED %+v\n", lh.hooks)
	return nil
}

func (lh *LuaHooks) Execute(col, op string, doc map[string]interface{}) (bool, map[string]interface{}, error) {
	fmt.Printf("HOOKS EXECUTE %v %v check\n", col, op)
	lh.Lock()
	defer lh.Unlock()
	ops, ok := lh.hooks[col]
	if !ok {
		return false, nil, nil
	}
	h, ok := ops[op]
	if !ok {
		return false, nil, nil
	}

	newDoc, err := h.Execute(doc)
	if err != nil {
		return true, nil, err
	}

	newDoc["_hooks"] = map[string]interface{}{
		op: h.ID[:7],
	}
	fmt.Printf("HOOKS EXECUTE %v %v executed\n", col, op)

	return true, newDoc, nil
}

func (lh *LuaHooks) Close() error {
	lh.L.Close()
	return nil
}

type LuaQueryEngine struct {
	storedQueries   map[string]*storedQuery // Stored query store
	storedQueryName string                  // Requested stored query name if any
	lfunc           *lua.LFunction

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

func SetLuaGlobals(L *lua.LState) {
	// FIXME(tsileo): a `use_index(index_field, value)` and have the optimizer use it
	// TODO(tsileo): harvesine function for geoquery
	// TODO(tsileo): current time helper
	L.SetGlobal("porterstemmer", L.NewFunction(ltokenize))
	L.SetGlobal("porterstemmer_stem", L.NewFunction(stem))
}

func (docstore *DocStore) newLuaQueryEngine(L *lua.LState, query *query) (*LuaQueryEngine, error) {
	if L == nil {
		L = lua.NewState()
	}
	engine := &LuaQueryEngine{
		storedQueries:   docstore.storedQueries,
		query:           query.storedQueryArgs,
		code:            queryToScript(query),
		storedQueryName: query.storedQuery,
		lfunc:           query.lfunc,
		L:               lua.NewState(),
		q:               lua.LNil,
		logger:          docstore.logger.New("submodule", "lua_query_engine"),
	}
	fmt.Printf("code=\n\n%s\n\n", engine.code)
	gluarequire2.NewRequire2Module(gluarequire2.NewRequireFromGitHub(nil)).SetGlobal(engine.L)
	SetLuaGlobals(engine.L)
	if err := engine.L.DoString(`
-- Python-like string.split implementation http://lua-users.org/wiki/SplitJoin
function string:split(sSeparator, nMax, bRegexp)
   assert(sSeparator ~= '')
   assert(nMax == nil or nMax >= 1)

   local aRecord = {}

   if self:len() > 0 then
      local bPlain = not bRegexp
      nMax = nMax or -1

      local nField, nStart = 1, 1
      local nFirst,nLast = self:find(sSeparator, nStart, bPlain)
      while nFirst and nMax ~= 0 do
         aRecord[nField] = self:sub(nStart, nFirst-1)
         nField = nField+1
         nStart = nLast+1
         nFirst,nLast = self:find(sSeparator, nStart, bPlain)
         nMax = nMax-1
      end
      aRecord[nField] = self:sub(nStart)
   end

   return aRecord
end
function get_path (doc, q)
  q = q:gsub('%[%d', '.%1')
  local parts = q:split('.')
  p = doc
  for _, part in ipairs(parts) do
    if type(p) ~= 'table' then
      return nil
    end
    if part:sub(1, 1) == '[' then
      part = part:sub(2, 2)
    end
    if tonumber(part) ~= nil then
      p = p[tonumber(part)]
    else
      p = p[part]
    end
    if p == nil then
      return nil
    end
  end
  return p
end
_G.get_path = get_path
function in_list (doc, path, value, q)
  local p = get_path(doc, path)
  if type(p) ~= 'table' then
    return false
  end
  for _, item in ipairs(p) do
    if q == nil then
      if item == value then return true end
    else
      if get_path(item, q) == value then return true end
    end
  end
  return false
end

function match (doc, path, op, value)
  p = get_path(doc, path)
  if type(p) ~= type(value) then return false end
  if op == 'EQ' then
    return p == value
  elseif op == 'NE' then
    return p ~= value
  elseif op == 'GT' then
    return p > value
  elseif op == 'GE' then
    return p >= value
  elseif op == 'LT' then
    return p < value
  elseif op == 'LE' then
    return p <= value
  end
  return false
end
_G.match = match

return in_list
`); err != nil {
		panic(err)
	}
	engine.L.SetGlobal("in_list", engine.L.Get(-1).(*lua.LFunction))
	engine.L.Pop(1)

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
	}
	if engine.lfunc != nil {
		ret = engine.lfunc
	}
	if ret == nil {
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
				fmt.Printf("failed to call match func: %+v %+v\n", doc, err)
				return false, err // FIXME(tsileo): a way to switch the return error/don't return error?
			}
			ret := engine.L.Get(-1)
			engine.L.Pop(1)
			if ret == lua.LTrue {
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
	var err error

	if lqe.matchFunc == nil {
		return false, fmt.Errorf("missing matchFunc")
	}

	if out, err = lqe.matchFunc(doc); err != nil {
		return false, err
	}

	lqe.logger.Debug("match code ran", "duration", time.Since(start))
	return out, nil
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

func stem(L *lua.LState) int {
	in := L.ToString(1)
	L.Push(lua.LString(porterstemmer.StemString(in)))
	return 1
}

// FIXME(tsileo): cache this and the stem, make it available to "apps" in a better way
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
