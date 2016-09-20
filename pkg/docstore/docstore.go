/*

Package docstore implements a JSON-based document store
built on top of the Versioned Key-Value store and the Blob store.

Each document will get assigned a MongoDB like ObjectId:

	<binary encoded uint32 (4 bytes) + 8 random bytes hex encoded >

The resulting id will have a length of 24 characters encoded as hex (12 raw bytes).

The JSON document will be stored as is and kvk entry will reference it.

	docstore:<collection>:<id> => <flag (1 byte) + JSON blob hash>

Document will be automatically sorted by creation time thanks to the ID.

The raw JSON will be stored as is, but the API will add the _id field on the fly.

*/
package docstore

import (
	"encoding/json"
	"errors"
	"fmt"
	"golang.org/x/net/context"
	"io/ioutil"
	"net/http"
	_ "path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/dchest/blake2b"
	"github.com/gorilla/mux"
	log "github.com/inconshreveable/log15"
	logext "github.com/inconshreveable/log15/ext"

	"github.com/tsileo/blobstash/pkg/blob"
	"github.com/tsileo/blobstash/pkg/blobstore"
	"github.com/tsileo/blobstash/pkg/config"
	"github.com/tsileo/blobstash/pkg/ctxutil"
	"github.com/tsileo/blobstash/pkg/docstore/id"
	_ "github.com/tsileo/blobstash/pkg/docstore/index"
	_ "github.com/tsileo/blobstash/pkg/docstore/optimizer"
	"github.com/tsileo/blobstash/pkg/httputil"
	"github.com/tsileo/blobstash/pkg/kvstore"
	"github.com/tsileo/blobstash/pkg/vkv"
)

// FIXME(tsileo): use the new vkv format for future GC, don't forget indexes
// and clean the full text index mess

// FIXME(tsileo): create a "meta" hook for handling indexing
// will need to solve few issues before:
// - do we need to check if the doc is already indexed?

// TODO(tsileo): add a <hexID>/_versions handler

var (
	PrefixKey    = "docstore:"
	PrefixKeyFmt = PrefixKey + "%s"
	KeyFmt       = PrefixKeyFmt + ":%s"

	PrefixIndexKeyFmt = "docstore-index:%s"
	IndexKeyFmt       = PrefixIndexKeyFmt + ":%s"

	PermName           = "docstore"
	PermCollectionName = "docstore:collection"
	PermWrite          = "write"
	PermRead           = "read"
)

func idFromKey(col, key string) (*id.ID, error) {
	hexID := strings.Replace(key, fmt.Sprintf("docstore:%s:", col), "", 1)
	_id, err := id.FromHex(hexID)
	if err != nil {
		return nil, err
	}
	return _id, err
}

const (
	FlagNoop byte = iota // Won't be indexed by Bleve
	FlagDeleted
)

const (
	ExpandJson = "#blobstash/json:"
)

type executionStats struct {
	NReturned           int    `json:"nReturned"`
	TotalDocsExamined   int    `json:"totalDocsExamined"`
	ExecutionTimeMillis int    `json:"executionTimeMillis"`
	LastID              string `json:"-"`
	Engine              string `json:"query_engine"`
	Index               string `json:"index"`
}

type DocStore struct {
	kvStore   *kvstore.KvStore
	blobStore *blobstore.BlobStore

	conf *config.Config
	// index    bleve.Index
	// docIndex *index.HashIndexes

	logger log.Logger
}

// New initializes the `DocStoreExt`
func New(logger log.Logger, conf *config.Config, kvStore *kvstore.KvStore, blobStore *blobstore.BlobStore) (*DocStore, error) {
	logger.Debug("init")
	// Try to load the docstore index (powered by a kv file)
	// docIndex, err := index.New()
	// if err != nil {
	// 	return nil, fmt.Errorf("failed to open the docstore index: %v", err)
	// }
	return &DocStore{
		kvStore:   kvStore,
		blobStore: blobStore,
		conf:      conf,
		// index:     index,
		// docIndex:  docIndex,
		logger: logger,
	}, nil
}

// Close closes all the open DB files.
func (docstore *DocStore) Close() error {
	// if err := docstore.docIndex.Close(); err != nil {
	// 	return err
	// }
	return nil
}

// RegisterRoute registers all the HTTP handlers for the extension
func (docstore *DocStore) Register(r *mux.Router, basicAuth func(http.Handler) http.Handler) {
	r.Handle("/", basicAuth(http.HandlerFunc(docstore.collectionsHandler())))
	r.Handle("/{collection}", basicAuth(http.HandlerFunc(docstore.docsHandler())))
	// r.Handle("/{collection}/_indexes", middlewares.Auth(http.HandlerFunc(docstore.indexesHandler())))
	r.Handle("/{collection}/{_id}", basicAuth(http.HandlerFunc(docstore.docHandler())))
}

// Expand a doc keys (fetch the blob as JSON, or a filesystem reference)
// e.g: {"ref": "@blobstash/json:<hash>"}
//      => {"ref": {"blob": "json decoded"}}
// XXX(tsileo): expanded ref must also works for marking a blob during GC
func (docstore *DocStore) expandKeys(doc map[string]interface{}) error {
	// docstore.logger.Info("expandKeys")
	for k, v := range doc {
		switch vv := v.(type) {
		case map[string]interface{}:
			if err := docstore.expandKeys(vv); err != nil {
				return err
			}
			continue
		case string:
			switch {
			case strings.HasPrefix(vv, ExpandJson):
				// XXX(tsileo): here and at other place, add a util func in hashutil to detect invalid string length at least
				blob, err := docstore.blobStore.Get(context.TODO(), vv[len(ExpandJson):])
				if err != nil {
					return fmt.Errorf("failed to expend JSON ref: \"%v => %v\": %v", ExpandJson, v, err)
				}
				expanded := map[string]interface{}{}
				if err := json.Unmarshal(blob, &expanded); err != nil {
					return fmt.Errorf("failed to unmarshal expanded blob  \"%v => %v\": %v", ExpandJson, v, err)
				}
				// docstore.logger.Info("expandKeys key expanded", "key", k, "val", expanded)
				expanded["hash"] = vv[len(ExpandJson):]
				doc[k] = expanded
			}
		}
	}
	return nil
}

// nextKey returns the next key for lexigraphical ordering (key = nextKey(lastkey))
func nextKey(key string) string {
	bkey := []byte(key)
	i := len(bkey)
	for i > 0 {
		i--
		bkey[i]++
		if bkey[i] != 0 {
			break
		}
	}
	return string(bkey)
}

// Collections returns all the existing collections
func (docstore *DocStore) Collections() ([]string, error) {
	collections := []string{}
	lastKey := ""
	for {
		ksearch := fmt.Sprintf("docstore:%v", lastKey)
		res, err := docstore.kvStore.Keys(context.TODO(), ksearch, "\xff", 50)
		// docstore.logger.Debug("loop", "ksearch", ksearch, "len_res", len(res))
		if err != nil {
			return nil, err
		}
		if len(res) == 0 {
			break
		}
		var col string
		for _, kv := range res {
			// Key = <docstore:{collection}:{_id}>
			col = strings.Split(kv.Key, ":")[1]
			collections = append(collections, col)
		}
		lastKey = nextKey(col)
	}
	return collections, nil
}

// HTTP handler to manage indexes for a collection
// func (docstore *DocStoreExt) indexesHandler() func(http.ResponseWriter, *http.Request) {
// 	return func(w http.ResponseWriter, r *http.Request) {
// 		vars := mux.Vars(r)
// 		collection := vars["collection"]
// 		if collection == "" {
// 			httputil.WriteJSONError(w, http.StatusInternalServerError, "Missing collection in the URL")
// 			return
// 		}

// 		// Ensure the client has the needed permissions
// 		permissions.CheckPerms(r, PermCollectionName, collection)

// 		switch r.Method {
// 		case "GET":
// 			// GET request, just list all the indexes
// 			srw := httputil.NewSnappyResponseWriter(w, r)
// 			indexes, err := docstore.Indexes(collection)
// 			if err != nil {
// 				panic(err)
// 			}
// 			httputil.WriteJSON(srw, indexes)
// 			srw.Close()
// 		case "POST":
// 			// POST request, create a new index from the body
// 			q := &index.Index{}
// 			if err := json.NewDecoder(r.Body).Decode(&q); err != nil {
// 				panic(err)
// 			}

// 			// Actually save the index
// 			if err := docstore.AddIndex(collection, q); err != nil {
// 				panic(err)
// 			}

// 			w.WriteHeader(http.StatusCreated)
// 		default:
// 			w.WriteHeader(http.StatusMethodNotAllowed)
// 		}
// 	}
// }

// HTTP handler for getting the collections list
func (docstore *DocStore) collectionsHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "GET":
			// Ensure the client has the needed permissions
			// permissions.CheckPerms(r, PermCollectionName)

			collections, err := docstore.Collections()
			if err != nil {
				panic(err)
			}

			srw := httputil.NewSnappyResponseWriter(w, r)
			httputil.WriteJSON(srw, map[string]interface{}{
				"collections": collections,
			})
			srw.Close()
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}
}

// isQueryAll returns `true` if there's no query.
func isQueryAll(q string) bool {
	if q == "" {
		return true
	}
	return false
}

// Indexes return the list of `Index` for the given collection
// func (docstore *DocStoreExt) Indexes(collection string) ([]*index.Index, error) {
// 	res, err := docstore.kvStore.ReversePrefixKeys(fmt.Sprintf(PrefixIndexKeyFmt, collection), "", "\xff", 50)
// 	indexes := []*index.Index{}
// 	if err != nil {
// 		panic(err)
// 	}
// 	for _, kv := range res {
// 		// FIXME(tsileo): this check shouldn't be here, it should be handled by ReversePrefixKeys!
// 		if !strings.HasPrefix(kv.Key, fmt.Sprintf(IndexKeyFmt, collection, "")) {
// 			break
// 		}
// 		index := &index.Index{ID: strings.Replace(kv.Key, fmt.Sprintf(IndexKeyFmt, collection, ""), "", 1)}
// 		if err := json.Unmarshal([]byte(kv.Value), index); err != nil {
// 			docstore.logger.Error("failed to unmarshal log entry", "err", err, "js", kv.Value)
// 			// return nil, err
// 			continue
// 		}
// 		indexes = append(indexes, index)
// 	}
// 	return indexes, nil
// }

// func (docstore *DocStoreExt) AddIndex(collection string, idx *index.Index) error {
// 	if len(idx.Fields) > 1 {
// 		return httputil.NewPublicErrorFmt("Only single field index are support for now")
// 	}

// 	var err error

// 	js, err := json.Marshal(idx)
// 	if err != nil {
// 		return err
// 	}

// 	// FIXME(tsileo): ensure we can't create duplicate index

// 	switch len(idx.Fields) {
// 	case 1:
// 		hashKey := fmt.Sprintf("single-field-%s", idx.Fields[0])
// 		_, err = docstore.kvStore.PutPrefix(fmt.Sprintf(PrefixIndexKeyFmt, collection), hashKey, string(js), -1, "")
// 	default:
// 		err = httputil.NewPublicErrorFmt("Bad index")
// 	}
// 	return err
// }

// IndexDoc indexes the given doc if needed, should never be called by the client,
// this method is exported to support re-indexing at the blob level and rebuild the index from it.
// func (docstore *DocStoreExt) IndexDoc(collection string, _id *id.ID, doc *map[string]interface{}) error {
// 	// Check if the document should be indexed by the full-text indexer (Bleve)
// 	if _id.Flag() == FlagFullTextIndexed {
// 		if err := docstore.index.Index(_id.String(), doc); err != nil {
// 			return err
// 		}
// 	}

// 	// Check if the document need to be indexed
// 	indexes, err := docstore.Indexes(collection)
// 	if err != nil {
// 		return fmt.Errorf("Failed to fetch index")
// 	}
// 	optz := optimizer.New(docstore.logger.New("module", "query optimizer"), indexes)
// 	shouldIndex, idx, idxKey := optz.ShouldIndex(*doc)
// 	if shouldIndex {
// 		docstore.logger.Debug("indexing document", "idx-key", idxKey, "_id", _id.String())
// 		// FIXME(tsileo): returns a special status code on `index.DuplicateIndexError`
// 		if err := docstore.docIndex.Index(collection, idx, idxKey, _id.String()); err != nil {
// 			return err
// 		}
// 	}

// 	return nil
// }

// Insert the given doc (`*map[string]interface{}` for now) in the given collection
func (docstore *DocStore) Insert(collection string, idoc interface{}) (*id.ID, error) {
	switch doc := idoc.(type) {
	case *map[string]interface{}:
		docFlag := FlagNoop
		data, err := json.Marshal(doc)
		if err != nil {
			return nil, err
		}
		// If there's already an "_id" field in the doc, remove it
		if _, ok := (*doc)["_id"]; ok {
			delete(*doc, "_id")
		}

		// Store the payload (JSON data) in a blob
		hash := fmt.Sprintf("%x", blake2b.Sum256(data))

		ctx := context.Background()

		blob := &blob.Blob{Hash: hash, Data: data}
		if err := docstore.blobStore.Put(ctx, blob); err != nil {
			return nil, err
		}

		// Build the ID and add some meta data
		now := time.Now().UTC()
		_id, err := id.New(int(now.Unix()))
		if err != nil {
			return nil, err
		}
		_id.SetHash(hash)
		_id.SetFlag(docFlag)

		// Create a pointer in the key-value store
		if _, err := docstore.kvStore.PutPrefix(ctx, fmt.Sprintf(PrefixKeyFmt, collection), _id.String(), hash, []byte{docFlag}, int(now.UnixNano())); err != nil {
			return nil, err
		}

		// Index the doc if needed
		// if err := docstore.IndexDoc(collection, _id, doc); err != nil {
		// 	docstore.logger.Error("Failed to index document", "_id", _id.String(), "err", err)
		// 	return _id, httputil.NewPublicErrorFmt("Failed to index document")
		// }

		return _id, nil
	}
	return nil, fmt.Errorf("doc must be a *map[string]interface{}")
}

type query struct {
	storedQuery     string
	storedQueryArgs interface{}
	basicQuery      string
	script          string
}

func queryToScript(q *query) string {
	if q.basicQuery != "" {
		return "if " + q.basicQuery + " then return true else return false end"
	}
	if q.script != "" {
		return q.script
	}
	// FIXME(tsileo): handle stored queries
	panic("shouldn't happen")

}

func (q *query) isMatchAll() bool {
	if q.script == "" && q.basicQuery == "" && q.storedQuery == "" && q.storedQueryArgs == nil {
		return true
	}
	return false
}

func (docstore *DocStore) Query(collection string, query *query, cursor string, limit int, res interface{}) (*executionStats, error) {
	js, stats, err := docstore.query(collection, query, cursor, limit)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(js, res); err != nil {
		return nil, err
	}
	return stats, nil
}

func addID(js []byte, _id string) []byte {
	// TODO(tsileo): add _updated/_created
	js2 := []byte(fmt.Sprintf("{\"_id\":\"%s\",", _id))
	js2 = append(js2, js[1:len(js)]...)
	return js2
}

// query returns a JSON list as []byte for the given query
// docs are unmarhsalled to JSON only when needed.
func (docstore *DocStore) query(collection string, query *query, cursor string, limit int) ([]byte, *executionStats, error) {
	// js := []byte("[")
	tstart := time.Now()
	stats := &executionStats{
		Engine: "lua",
	}

	// Check if the query can be optimized thanks to an already present index
	// indexes, err := docstore.Indexes(collection)
	// if err != nil {
	// 	return nil, nil, fmt.Errorf("Failed to fetch index")
	// }
	// optz := optimizer.New(docstore.logger.New("module", "query optimizer"), indexes)

	// Handle the cursor
	start := ""
	if cursor != "" {
		start = fmt.Sprintf(KeyFmt, collection, cursor)
	}
	end := "\xff"

	// Tweak the query limit
	fetchLimit := limit
	isMatchAll := query.isMatchAll()
	if isMatchAll {
		stats.Engine = "match_all"
	} else {
		// Prefetch more docs since there's a lot of chance the query won't
		// match every documents
		fetchLimit = int(float64(limit) * 1.3)
	}

	qLogger := docstore.logger.New("query", query, "query_engine", stats.Engine, "id", logext.RandId(8))
	qLogger.Info("new query")
	docs := []map[string]interface{}{}

	// Select the optimizer i.e. should we use an index?
	// optzType, optzIndex := optz.Select(query)
	// stats.Optimizer = optzType
	// if optzIndex != nil {
	// stats.Index = optzIndex.ID
	// }

	var lastKey string
QUERY:
	for {
		// Loop until we have the number of requested documents, or if we scanned everything
		qLogger.Debug("internal query", "limit", limit, "cursor", cursor, "start", start, "end", end, "nreturned", stats.NReturned)
		// FIXME(tsileo): use `PrefixKeys` if ?sort=_id (-_id by default).
		_ids := []*id.ID{}
		// switch optzType {
		// case optimizer.Linear:
		// Performs a unoptimized linear scan
		res, err := docstore.kvStore.ReversePrefixKeys(fmt.Sprintf(PrefixKeyFmt, collection), start, end, fetchLimit)
		if err != nil {
			panic(err)
		}
		for _, kv := range res {
			_id, err := idFromKey(collection, kv.Key)
			if err != nil {
				return nil, nil, err
			}
			_ids = append(_ids, _id)
			lastKey = kv.Key
		}
		// case optimizer.Index:
		// 	// Use the index to answer the query
		// 	// FIXME(tsileo): make cursor works (start, end handling)
		// 	optzIndexHash := index.IndexKey(query[optzIndex.Fields[0]])
		// 	_ids, err = docstore.docIndex.Iter(collection, optzIndex, optzIndexHash, start, end, fetchLimit)
		// 	if err != nil {
		// 		panic(err)
		// 	}
		// }
		var qmatcher QueryMatcher
		switch stats.Engine {
		case "match_all":
			qmatcher = &MatchAllEngine{}
		case "lua":
			// FIXME(tsileo): handle stored queries
			qmatcher, err = NewLuaQueryEngine(queryToScript(query), "", query.storedQueryArgs)
			if err != nil {
				return nil, stats, err
			}
		default:
			panic("shouldn't happen")
		}
		defer qmatcher.Close()

		// FIXME(tsileo): remake `_ids` a list of string
		for _, _id := range _ids {
			// Check if the doc match the query
			// jsPart := []byte{}
			doc := map[string]interface{}{}
			qLogger.Debug("fetch doc", "_id", _id)
			var err error
			if _id, err = docstore.Fetch(collection, _id.String(), &doc); err != nil {
				if err == vkv.ErrNotFound {
					break
				}
				panic(err)
			}
			stats.TotalDocsExamined++
			ok, err := qmatcher.Match(doc)
			if err != nil {
				return nil, stats, err
			}
			if ok {
				doc["_id"] = _id

				doc["_created"] = _id.Ts()
				updated := _id.Version() / 1e9
				if updated != doc["_created"] {
					doc["_updated"] = updated
				}
				docs = append(docs, doc)
				stats.NReturned++
				stats.LastID = _id.String()
				if stats.NReturned == limit {
					break QUERY
				}
			}
		}
		if len(_ids) == 0 || len(_ids) < limit {
			break
		}
		start = nextKey(lastKey)
	}
	// Remove the last comma fron the JSON string
	// if stats.NReturned > 0 {
	// js = js[0 : len(js)-1]
	// }
	js, err := json.Marshal(docs)
	if err != nil {
		return nil, nil, err
	}

	duration := time.Since(tstart)
	qLogger.Debug("scan done", "duration", duration, "nReturned", stats.NReturned, "scanned", stats.TotalDocsExamined)
	// XXX(tsileo): display duration in string format or nanosecond precision??
	stats.ExecutionTimeMillis = int(duration.Nanoseconds() / 1e6)
	return js, stats, nil
}

// HTTP handler for the collection (handle listing+query+insert)
func (docstore *DocStore) docsHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		vars := mux.Vars(r)
		collection := vars["collection"]
		if collection == "" {
			httputil.WriteJSONError(w, http.StatusInternalServerError, "Missing collection in the URL")
			return
		}
		switch r.Method {
		case "GET", "HEAD":
			// permissions.CheckPerms(r, PermCollectionName, collection, PermRead)

			// Parse the cursor
			cursor := q.Get("cursor")

			// Parse the query (JSON-encoded)
			var queryArgs interface{}
			jsQuery := q.Get("stored_query_args")
			if jsQuery != "" {
				if err := json.Unmarshal([]byte(jsQuery), &queryArgs); err != nil {
					httputil.WriteJSONError(w, http.StatusInternalServerError, "Failed to decode JSON query")
					return
				}
			}

			// Parse the limit
			limit := 50
			if q.Get("limit") != "" {
				ilimit, err := strconv.Atoi(q.Get("limit"))
				if err != nil {
					http.Error(w, "bad limit", 500)
				}
				limit = ilimit
			}
			js, stats, err := docstore.query(collection, &query{
				storedQueryArgs: queryArgs,
				storedQuery:     q.Get("stored_query"),
				script:          q.Get("script"),
				basicQuery:      q.Get("query"),
			}, cursor, limit)
			if err != nil {
				panic(err)
			}

			// Set some meta headers to help the client build subsequent query
			// (iterator/cursor handling)
			var hasMore bool
			// Guess if they're are still results on client-side,
			// by checking if NReturned < limit, we can deduce there's no more results.
			// The cursor should be the start of the next query
			if stats.NReturned == limit {
				hasMore = true
			}
			w.Header().Set("BlobStahs-DocStore-Iter-Has-More", strconv.FormatBool(hasMore))
			w.Header().Set("BlobStash-DocStore-Iter-Cursor", nextKey(stats.LastID))

			// w.Header().Set("BlobStash-DocStore-Query-Optimizer", stats.Optimizer)
			// if stats.Optimizer != optimizer.Linear {
			// 	w.Header().Set("BlobStash-DocStore-Query-Index", stats.Index)
			// }

			// Set headers for the query stats
			w.Header().Set("BlobStash-DocStore-Query-Engine", stats.Engine)
			w.Header().Set("BlobStash-DocStore-Query-Returned", strconv.Itoa(stats.NReturned))
			w.Header().Set("BlobStash-DocStore-Query-Examined", strconv.Itoa(stats.TotalDocsExamined))
			w.Header().Set("BlobStash-DocStore-Query-Exec-Time", strconv.Itoa(stats.ExecutionTimeMillis))

			w.Header().Set("BlobStash-DocStore-Results-Count", strconv.Itoa(stats.NReturned))

			// This way, HEAD request can acts as a count query
			if r.Method == "HEAD" {
				return
			}

			// Write the JSON response (encoded if requested)
			w.Header().Set("Content-Type", "application/json")
			srw := httputil.NewSnappyResponseWriter(w, r)
			srw.Write(js)
			srw.Close()
		case "POST":
			// permissions.CheckPerms(r, PermCollectionName, collection, PermWrite)
			// Read the whole body
			blob, err := ioutil.ReadAll(r.Body)
			if err != nil {
				panic(err)
			}
			// Ensure it's JSON encoded
			doc := map[string]interface{}{}
			if err := json.Unmarshal(blob, &doc); err != nil {
				docstore.logger.Error("Failed to parse JSON input", "collection", collection, "err", err)
				panic(httputil.NewPublicErrorFmt("Invalid JSON document"))
			}
			// XXX(tsileo): Ensures there's no field starting with "_", e.g. `_id`/`_created`/`_updated`

			// Actually insert the doc
			_id, err := docstore.Insert(collection, &doc)
			if err != nil {
				panic(err)
			}

			// Output some headers
			w.Header().Set("BlobStash-DocStore-Doc-Id", _id.String())
			w.Header().Set("BlobStash-DocStore-Doc-Hash", _id.Hash())
			w.Header().Set("BlobStash-DocStore-Doc-CreatedAt", strconv.Itoa(_id.Ts()))
			w.WriteHeader(http.StatusCreated)
			srw := httputil.NewSnappyResponseWriter(w, r)
			httputil.WriteJSON(srw, map[string]interface{}{
				"_id":      _id.String(),
				"_created": _id.Ts(),
			})
			srw.Close()

		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}
}

// Fetch a single document into `res` and returns the `id.ID`
func (docstore *DocStore) Fetch(collection, sid string, res interface{}) (*id.ID, error) {
	if collection == "" {
		return nil, errors.New("missing collection query arg")
	}

	// Fetch the VKV entry for this _id
	kv, err := docstore.kvStore.Get(context.TODO(), fmt.Sprintf(KeyFmt, collection, sid), -1)
	if err != nil {
		return nil, err
	}

	// Parse the ID
	_id, err := id.FromHex(sid)
	if err != nil {
		return nil, fmt.Errorf("invalid _id: %v", err)
	}

	// Extract the hash (first byte is the Flag)
	// XXX(tsileo): add/handle a `Deleted` flag
	hash := kv.Hash
	// kv.Value[1:len(kv.Value)]

	// Fetch the blob
	blob, err := docstore.blobStore.Get(context.TODO(), hash)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch blob %v", hash)
	}

	// Build the doc
	switch idoc := res.(type) {
	case *map[string]interface{}:
		if err := json.Unmarshal(blob, idoc); err != nil {
			return nil, fmt.Errorf("failed to unmarshal blob: %s", blob)
		}
		if err := docstore.expandKeys(*idoc); err != nil {
			return nil, err
		}
	case *[]byte:
		// Just the copy if JSON if a []byte is provided
		*idoc = append(*idoc, blob...)
	}
	_id.SetHash(hash)
	_id.SetFlag(byte(kv.Data[0]))
	_id.SetVersion(kv.Version)
	return _id, nil
}

// HTTP handler for serving/updating a single doc
func (docstore *DocStore) docHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		collection := vars["collection"]
		if collection == "" {
			httputil.WriteJSONError(w, http.StatusInternalServerError, "Missing collection in the URL")
			return
		}
		sid := vars["_id"]
		if sid == "" {
			httputil.WriteJSONError(w, http.StatusInternalServerError, "Missing _id in the URL")
			return
		}
		var _id *id.ID
		var err error
		srw := httputil.NewSnappyResponseWriter(w, r)
		defer srw.Close()
		switch r.Method {
		case "GET", "HEAD":
			// Serve the document JSON encoded
			// permissions.CheckPerms(r, PermCollectionName, collection, PermRead)
			js := []byte{}
			if _id, err = docstore.Fetch(collection, sid, &js); err != nil {
				if err == vkv.ErrNotFound {
					// Document doesn't exist, returns a status 404
					w.WriteHeader(http.StatusNotFound)
					return
				}
				panic(err)
			}

			if r.Method == "GET" {
				w.Header().Set("Content-Type", "application/json")
				srw.Write(addID(js, sid))
			}
		case "POST":
			// Update the document
			// permissions.CheckPerms(r, PermCollectionName, collection, PermWrite)
			ns := r.Header.Get("BlobStash-Namespace")
			ctx := ctxutil.WithNamespace(context.Background(), ns)
			// Fetch the actual doc
			doc := map[string]interface{}{}
			if _id, err = docstore.Fetch(collection, sid, &doc); err != nil {
				if err == vkv.ErrNotFound {
					// Document doesn't exist, returns a status 404
					w.WriteHeader(http.StatusNotFound)
					return
				}
				panic(err)
			}

			// Parse the update query
			var update map[string]interface{}
			decoder := json.NewDecoder(r.Body)
			if err := decoder.Decode(&update); err != nil {
				panic(httputil.NewPublicErrorFmt("Invalid JSON input"))
			}

			docstore.logger.Debug("Update", "_id", sid, "ns", ns, "update_query", update)
			// XXX(tsileo): no more $set, move to PATCH and make it acts like $set???

			// Actually update the doc
			newDoc, err := updateDoc(doc, update)

			if _, ok := newDoc["_id"]; ok {
				delete(newDoc, "_id")
			}

			if _, ok := newDoc["_created"]; ok {
				delete(newDoc, "_created")
			}

			if _, ok := newDoc["_updated"]; ok {
				delete(newDoc, "_updated")
			}

			// Marshal it to JSON to save it back as a blob
			data, err := json.Marshal(newDoc)
			if err != nil {
				panic(err)
			}

			// Compute the Blake2B hash and save the blob
			hash := fmt.Sprintf("%x", blake2b.Sum256(data))
			blob := &blob.Blob{Hash: hash, Data: data}
			if err := docstore.blobStore.Put(ctx, blob); err != nil {
				panic(err)
			}

			// XXX(tsileo): allow to update the flag?

			kv, err := docstore.kvStore.Put(ctx, fmt.Sprintf(KeyFmt, collection, _id.String()), hash, []byte{_id.Flag()}, -1)
			if err != nil {
				panic(err)
			}
			newDoc["_id"] = _id.String()
			newDoc["_created"] = _id.Ts()
			newDoc["_updated"] = kv.Version / 1e9
			httputil.WriteJSON(srw, newDoc)
		case "DELETE":
			// FIXME(tsileo): empty the key, and hanlde it in the get/query
		}

		w.Header().Set("BlobStash-DocStore-Doc-Id", sid)
		w.Header().Set("BlobStash-DocStore-Doc-Hash", _id.Hash())
		w.Header().Set("BlobStash-DocStore-Doc-CreatedAt", strconv.Itoa(_id.Ts()))
	}
}
