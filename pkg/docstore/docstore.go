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
package docstore // import "a4.io/blobstash/pkg/docstore"

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/dchest/blake2b"
	"github.com/evanphx/json-patch"
	"github.com/gorilla/mux"
	log "github.com/inconshreveable/log15"
	logext "github.com/inconshreveable/log15/ext"
	"github.com/vmihailenco/msgpack"

	"a4.io/blobstash/pkg/blob"
	"a4.io/blobstash/pkg/config"
	"a4.io/blobstash/pkg/docstore/id"
	_ "a4.io/blobstash/pkg/docstore/index"
	"a4.io/blobstash/pkg/filetree"
	"a4.io/blobstash/pkg/httputil"
	"a4.io/blobstash/pkg/httputil/bewit"
	"a4.io/blobstash/pkg/stash/store"
	"a4.io/blobstash/pkg/vkv"
	_ "github.com/tsileo/blobstash/pkg/docstore/optimizer"
)

// FIXME(tsileo): create a "meta" hook for handling indexing
// will need to solve few issues before:
// - do we need to check if the doc is already indexed?

var (
	PrefixKey    = "docstore:"
	PrefixKeyFmt = PrefixKey + "%s"
	KeyFmt       = PrefixKeyFmt + ":%s"

	PrefixIndexKeyFmt = "docstore-index:%s"
	IndexKeyFmt       = PrefixIndexKeyFmt + ":%s"

	// XXX(tsileo): remove or re-implement fine-grained permission
	PermName           = "docstore"
	PermCollectionName = "docstore:collection"
	PermWrite          = "write"
	PermRead           = "read"
)

var reservedKeys = map[string]struct{}{
	"_id":      struct{}{},
	"_updated": struct{}{},
	"_created": struct{}{},
	"_hash":    struct{}{},
}

func idFromKey(col, key string) (*id.ID, error) {
	hexID := strings.Replace(key, fmt.Sprintf("docstore:%s:", col), "", 1)
	_id, err := id.FromHex(hexID)

	if err != nil {
		return nil, err
	}
	return _id, err
}

const (
	FlagNoop byte = iota // Default flag
	FlagDeleted
)

const (
	PointerBlobJSON = "@blobs/json:" // FIXME(tsileo): document the Pointer feature
	// PointerBlobRef     = "@blobs/ref:"  // FIXME(tsileo): implements this like a @filetree/ref
	PointerFiletreeRef = "@filetree/ref:"
	PointerURLInfo     = "@url/info:" // FIXME(tsileo): fetch OG meta data or at least title, optionally screenshot???
	// TODO(tsileo): implements PointerKvRef
	// PointerKvRef = "@kv/ref:"
	// XXX(tsileo): allow custom Lua-defined pointer, this could be useful for implement cross-note linking in Blobs

	// Sharing TTL for the bewit link of Filetree references
	shareDuration = 30 * time.Minute
)

type executionStats struct {
	NReturned         int    `json:"nReturned"`
	TotalDocsExamined int    `json:"totalDocsExamined"`
	ExecutionTimeNano int64  `json:"executionTimeNano"`
	LastID            string `json:"-"`
	Engine            string `json:"query_engine"`
	Index             string `json:"index"`
}

type DocStore struct {
	kvStore   store.KvStore
	blobStore store.BlobStore
	filetree  *filetree.FileTreeExt

	conf *config.Config
	// docIndex *index.HashIndexes

	storedQueries map[string]*storedQuery

	locker *locker

	logger log.Logger
}

type storedQuery struct {
	Name string
	Main string
}

// New initializes the `DocStoreExt`
func New(logger log.Logger, conf *config.Config, kvStore store.KvStore, blobStore store.BlobStore, ft *filetree.FileTreeExt) (*DocStore, error) {
	logger.Debug("init")
	// Try to load the docstore index (powered by a kv file)
	// docIndex, err := index.New()
	// if err != nil {
	// 	return nil, fmt.Errorf("failed to open the docstore index: %v", err)
	// }

	// Load the docstore's stored queries from the config
	storedQueries := map[string]*storedQuery{}
	if conf.Docstore != nil && conf.Docstore.StoredQueries != nil {
		for _, squery := range conf.Docstore.StoredQueries {
			// First ensure the required match.lua is present
			if _, err := os.Stat(filepath.Join(squery.Path, "main.lua")); os.IsNotExist(err) {
				return nil, fmt.Errorf("missing `main.lua` for stored query %s", squery.Name)
			}

			storedQuery := &storedQuery{
				Name: squery.Name,
				Main: filepath.Join(squery.Path, "main.lua"),
			}
			storedQueries[squery.Name] = storedQuery
		}
	}

	return &DocStore{
		kvStore:       kvStore,
		blobStore:     blobStore,
		filetree:      ft,
		storedQueries: storedQueries,
		conf:          conf,
		locker:        newLocker(),
		logger:        logger,
		// docIndex:  docIndex,
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
	r.Handle("/_stored_queries", basicAuth(http.HandlerFunc(docstore.storedQueriesHandler())))

	r.Handle("/{collection}", basicAuth(http.HandlerFunc(docstore.docsHandler())))
	// r.Handle("/{collection}/_indexes", middlewares.Auth(http.HandlerFunc(docstore.indexesHandler())))
	r.Handle("/{collection}/{_id}", basicAuth(http.HandlerFunc(docstore.docHandler())))
	r.Handle("/{collection}/{_id}/_versions", basicAuth(http.HandlerFunc(docstore.docVersionsHandler())))
}

// Expand a doc keys (fetch the blob as JSON, or a filesystem reference)
// e.g: {"ref": "@blobstash/json:<hash>"}
//      => {"ref": {"blob": "json decoded"}}
// XXX(tsileo): expanded ref must also works for marking a blob during GC
// FIXME(tsileo): rename this to "pointers" and return {"data":{[...]}, "pointers": {}}
func (docstore *DocStore) fetchPointers(doc map[string]interface{}) (map[string]interface{}, error) {
	pointers := map[string]interface{}{}
	// docstore.logger.Info("expandKeys")

	for _, v := range doc {
		switch vv := v.(type) {
		case map[string]interface{}:
			docPointers, err := docstore.fetchPointers(vv)
			if err != nil {
				return nil, err
			}
			for k, v := range docPointers {
				pointers[k] = v
			}
			continue
		case string:
			switch {
			case strings.HasPrefix(vv, PointerBlobJSON):
				if _, ok := pointers[vv]; ok {
					// The reference has already been fetched
					continue
				}
				// XXX(tsileo): here and at other place, add a util func in hashutil to detect invalid string length at least
				blob, err := docstore.blobStore.Get(context.TODO(), vv[len(PointerBlobJSON):])
				if err != nil {
					return nil, fmt.Errorf("failed to fetch JSON ref: \"%v => %v\": %v", PointerBlobJSON, v, err)
				}
				p := map[string]interface{}{}
				if err := json.Unmarshal(blob, &p); err != nil {
					return nil, fmt.Errorf("failed to unmarshal blob  \"%v => %v\": %v", PointerBlobJSON, v, err)
				}
				pointers[vv] = p
			case strings.HasPrefix(vv, PointerFiletreeRef):
				if _, ok := pointers[vv]; ok {
					// The reference has already been fetched
					continue
				}
				// XXX(tsileo): here and at other place, add a util func in hashutil to detect invalid string length at least
				hash := vv[len(PointerFiletreeRef):]
				// TODO(tsileo): call filetree to get a node
				// blob, err := docstore.blobStore.Get(context.TODO(), hash)
				// if err != nil {
				// 	return nil, fmt.Errorf("failed to fetch JSON ref: \"%v => %v\": %v", PointerFiletreeRef, v, err)
				// }

				// // Reconstruct the Meta
				// var p map[string]interface{}
				// if err := json.Unmarshal(blob, &p); err != nil {
				// 	return nil, fmt.Errorf("failed to unmarshal meta  \"%v => %v\": %v", PointerBlobJSON, v, err)
				// }
				node, err := docstore.filetree.Node(context.TODO(), hash)
				if err != nil {
					return nil, err
				}

				// Create a temporary authorization for the file (with a bewit)
				u := &url.URL{Path: fmt.Sprintf("/%s/%s", node.Type[0:1], hash)}
				if err := bewit.Bewit(docstore.filetree.SharingCred(), u, shareDuration); err != nil {
					return nil, fmt.Errorf("failed to generate bewit: %v")
				}
				node.URL = u.String()

				pointers[vv] = node
			}

		}
	}

	return pointers, nil
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
	index := map[string]struct{}{}
	var lastKey string
	ksearch := fmt.Sprintf("docstore:%v", lastKey)
	for {
		res, cursor, err := docstore.kvStore.Keys(context.TODO(), ksearch, "docstore:\xff", 0)
		ksearch = cursor
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
			index[col] = struct{}{}
		}
	}
	for col, _ := range index {
		collections = append(collections, col)
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

// HTTP handler for checking the loaded saved queries
func (docstore *DocStore) storedQueriesHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "GET":
			httputil.WriteJSON(w, docstore.storedQueries)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}
}

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
func (docstore *DocStore) Insert(collection string, doc *map[string]interface{}) (*id.ID, error) {
	docFlag := FlagNoop
	// If there's already an "_id" field in the doc, remove it
	if _, ok := (*doc)["_id"]; ok {
		delete(*doc, "_id")
	}
	data, err := msgpack.Marshal(doc)
	if err != nil {
		return nil, err
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
	_id, err := id.New(now.UnixNano())
	if err != nil {
		return nil, err
	}
	_id.SetHash(hash)
	_id.SetFlag(docFlag)

	// Create a pointer in the key-value store
	if _, err := docstore.kvStore.Put(
		ctx, fmt.Sprintf(KeyFmt, collection, _id.String()), hash, []byte{docFlag}, int(now.UnixNano()),
	); err != nil {
		return nil, err
	}

	// Index the doc if needed
	// if err := docstore.IndexDoc(collection, _id, doc); err != nil {
	// 	docstore.logger.Error("Failed to index document", "_id", _id.String(), "err", err)
	// 	return _id, httputil.NewPublicErrorFmt("Failed to index document")
	// }

	return _id, nil
}

type query struct {
	storedQuery     string
	storedQueryArgs interface{}
	basicQuery      string
	script          string
}

func queryToScript(q *query) string {
	if q.basicQuery != "" {
		return `return function(doc)
  if ` + q.basicQuery + ` then return true else return false end
end
`
	}
	if q.script != "" {
		return q.script
	}
	// Must be a stored query, return an empty string
	return ""
}

func (q *query) isMatchAll() bool {
	if q.script == "" && q.basicQuery == "" && q.storedQuery == "" && q.storedQueryArgs == nil {
		return true
	}
	return false
}

func addSpecialFields(doc map[string]interface{}, _id *id.ID) {
	doc["_id"] = _id
	doc["_hash"] = _id.Hash()

	doc["_created"] = time.Unix(0, _id.Ts()).UTC().Format(time.RFC3339)
	updated := _id.Version()
	if updated != doc["_created"] {
		doc["_updated"] = time.Unix(0, int64(updated)).UTC().Format(time.RFC3339)
	}
}

func (docstore *DocStore) Query(collection string, query *query, cursor string, limit, asOf int) ([]map[string]interface{}, map[string]interface{}, *executionStats, error) {
	docs, pointers, stats, err := docstore.query(collection, query, cursor, limit, true, asOf)
	if err != nil {
		return nil, nil, nil, err
	}
	// TODO(tsileo): fix this
	return docs, pointers, stats, nil
}

// query returns a JSON list as []byte for the given query
// docs are unmarhsalled to JSON only when needed.
func (docstore *DocStore) query(collection string, query *query, cursor string, limit int, fetchPointers bool, asOf int) ([]map[string]interface{}, map[string]interface{}, *executionStats, error) {
	// js := []byte("[")
	tstart := time.Now()
	stats := &executionStats{
		Engine: "lua", // XXX(ts): should not be a string
	}

	pointers := map[string]interface{}{}

	// Check if the query can be optimized thanks to an already present index
	// indexes, err := docstore.Indexes(collection)
	// if err != nil {
	// 	return nil, nil, fmt.Errorf("Failed to fetch index")
	// }
	// optz := optimizer.New(docstore.logger.New("module", "query optimizer"), indexes)

	// Handle the cursor
	start := fmt.Sprintf(KeyFmt, collection, "\xff")
	if cursor != "" {
		start = fmt.Sprintf(KeyFmt, collection, cursor)
	}
	end := fmt.Sprintf(KeyFmt, collection, "")

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

QUERY:
	for {
		// Loop until we have the number of requested documents, or if we scanned everything
		qLogger.Debug("internal query", "limit", limit, "cursor", cursor, "start", start, "end", end, "nreturned", stats.NReturned)
		// FIXME(tsileo): use `PrefixKeys` if ?sort=_id (-_id by default).
		_ids := []*id.ID{}
		// switch optzType {
		// case optimizer.Linear:
		// Performs a unoptimized linear scan
		// res, cursor, err := docstore.kvStore.Keys(context.TODO(), end, start, fetchLimit)
		res, cursor, err := docstore.kvStore.ReverseKeys(context.TODO(), end, start, fetchLimit)
		// res, err := docstore.kvStore.ReverseKeys(end, start, fetchLimit)
		if err != nil {
			panic(err)
		}
		for _, kv := range res {
			_id, err := idFromKey(collection, kv.Key)
			if err != nil {
				return nil, nil, nil, err
			}
			_ids = append(_ids, _id)
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
			qmatcher, err = docstore.newLuaQueryEngine(query)
			if err != nil {
				return nil, nil, stats, err
			}
		default:
			panic("shouldn't happen")
		}
		defer qmatcher.Close()
		var docPointers map[string]interface{}
		for _, _id := range _ids {
			// Check if the doc match the query
			// jsPart := []byte{}
			doc := map[string]interface{}{}
			qLogger.Debug("fetch doc", "_id", _id, "as_of", asOf)
			var err error
			if asOf > 0 {
				// FIXME(tsileo): should return a `[]*id.ID` to, and check the flag before selecting the doc
				docVersions, allDocPointers, _, err := docstore.FetchVersions(collection, _id.String(), asOf, 1, fetchPointers)
				// FIXME(tsileo): check deleted
				if err != nil {
					panic(err)
				}
				if len(docVersions) > 0 {
					doc = docVersions[0]
					docPointers = allDocPointers
				} else {
					continue
				}
			} else {
				// FIXME(tsileo): only fetch the pointers once the doc has been matched!
				if _id, docPointers, err = docstore.Fetch(collection, _id.String(), &doc, fetchPointers, -1); err != nil {

					// The document is deleted skip it
					if _id.Flag() == FlagDeleted {
						continue
					}
					// TODO(tsileo): why catch ErrNotFound? should panic?
					if err == vkv.ErrNotFound {
						break
					}
					panic(err)
				}
			}
			stats.TotalDocsExamined++
			ok, err := qmatcher.Match(doc)
			if err != nil {
				return nil, nil, stats, err
			}
			if ok {
				addSpecialFields(doc, _id)
				if fetchPointers {
					for k, v := range docPointers {
						pointers[k] = v
					}
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
		start = cursor
	}

	duration := time.Since(tstart)
	qLogger.Debug("scan done", "duration", duration, "nReturned", stats.NReturned, "scanned", stats.TotalDocsExamined)
	stats.ExecutionTimeNano = duration.Nanoseconds()
	return docs, pointers, stats, nil
}

// HTTP handler for the collection (handle listing+query+insert)
func (docstore *DocStore) docsHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		q := httputil.NewQuery(r.URL.Query())
		vars := mux.Vars(r)
		collection := vars["collection"]
		if collection == "" {
			httputil.WriteJSONError(w, http.StatusInternalServerError, "Missing collection in the URL")
			return
		}
		switch r.Method {
		case "GET", "HEAD":
			// permissions.CheckPerms(r, PermCollectionName, collection, PermRead)

			var asOf int
			var err error
			// Parse the cursor
			cursor := q.Get("cursor")
			if v := q.Get("as_of"); v != "" {
				t, err := time.Parse("2006-1-2 15:4:5", v)
				if err != nil {
					panic(err)
				}
				asOf = int(t.UTC().UnixNano())
			}
			if asOf == 0 {
				asOf, err = q.GetIntDefault("as_of_nano", 0)
				if err != nil {
					panic(err)
				}
			}

			// Parse the query (JSON-encoded)
			var queryArgs interface{}
			jsQuery := q.Get("stored_query_args")
			if jsQuery != "" {
				if err := json.Unmarshal([]byte(jsQuery), &queryArgs); err != nil {
					httputil.WriteJSONError(w, http.StatusInternalServerError, "Failed to decode JSON query")
					return
				}
			}

			limit, err := q.GetInt("limit", 50, 1000)
			if err != nil {
				httputil.Error(w, err)
				return
			}

			docs, pointers, stats, err := docstore.query(collection, &query{
				storedQueryArgs: queryArgs,
				storedQuery:     q.Get("stored_query"),
				script:          q.Get("script"),
				basicQuery:      q.Get("query"),
			}, cursor, limit, true, asOf)
			if err != nil {
				panic(err)
				docstore.logger.Error("query failed", "err", err)
				httputil.Error(w, err)
				return
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
			w.Header().Set("BlobStash-DocStore-Iter-Has-More", strconv.FormatBool(hasMore))
			w.Header().Set("BlobStash-DocStore-Iter-Cursor", vkv.PrevKey(stats.LastID))

			// w.Header().Set("BlobStash-DocStore-Query-Optimizer", stats.Optimizer)
			// if stats.Optimizer != optimizer.Linear {
			// 	w.Header().Set("BlobStash-DocStore-Query-Index", stats.Index)
			// }

			// Set headers for the query stats
			w.Header().Set("BlobStash-DocStore-Query-Engine", stats.Engine)
			w.Header().Set("BlobStash-DocStore-Query-Returned", strconv.Itoa(stats.NReturned))
			w.Header().Set("BlobStash-DocStore-Query-Examined", strconv.Itoa(stats.TotalDocsExamined))
			w.Header().Set("BlobStash-DocStore-Query-Exec-Time-Nano", strconv.FormatInt(stats.ExecutionTimeNano, 10))

			w.Header().Set("BlobStash-DocStore-Results-Count", strconv.Itoa(stats.NReturned))

			// This way, HEAD request can acts as a count query
			if r.Method == "HEAD" {
				return
			}

			js, err := json.Marshal(&map[string]interface{}{
				"pointers": pointers,
				"data":     docs,
				"pagination": map[string]interface{}{
					"cursor":   vkv.PrevKey(stats.LastID),
					"has_more": hasMore,
					"count":    stats.NReturned,
					"per_page": limit,
				},
			})

			if err != nil {
				panic(err)
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

			// Check for reserved keys
			for k, _ := range doc {
				if _, ok := reservedKeys[k]; ok {
					// XXX(tsileo): delete them or raises an exception?
					delete(doc, k)
				}
			}

			// Actually insert the doc
			_id, err := docstore.Insert(collection, &doc)
			if err != nil {
				panic(err)
			}

			// Output some headers
			w.Header().Set("BlobStash-DocStore-Doc-Id", _id.String())
			w.Header().Set("BlobStash-DocStore-Doc-Hash", _id.Hash())
			w.Header().Set("BlobStash-DocStore-Doc-CreatedAt", strconv.FormatInt(_id.Ts(), 10))
			w.WriteHeader(http.StatusCreated)
			srw := httputil.NewSnappyResponseWriter(w, r)

			created := time.Unix(0, _id.Ts()).UTC().Format(time.RFC3339)

			httputil.WriteJSON(srw, map[string]interface{}{
				"_id":      _id.String(),
				"_created": created,
				"_hash":    _id.Hash(),
			})
			srw.Close()

		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}
}

// FIXME(ts): A way to make pointers optional?

// Fetch a single document into `res` and returns the `id.ID`
// Start acts like a cursor.
func (docstore *DocStore) FetchVersions(collection, sid string, start, limit int, fetchPointers bool) ([]map[string]interface{}, map[string]interface{}, int, error) {
	var cursor int
	// TODO(tsileo): better output than a slice of `map[string]interface{}`
	if collection == "" {
		return nil, nil, cursor, errors.New("missing collection query arg")
	}

	// Fetch the KV versions entry for this _id
	// XXX(tsileo): use int64 for start/end
	kvv, _, err := docstore.kvStore.Versions(context.TODO(), fmt.Sprintf(KeyFmt, collection, sid), start, limit)
	// FIXME(tsileo): return the cursor from Versions
	if err != nil {
		return nil, nil, cursor, err
	}

	// Parse the ID
	// _id, err := id.FromHex(sid)
	// if err != nil {
	// 	return nil, nil, fmt.Errorf("invalid _id: %v", err)
	// }
	docs := []map[string]interface{}{}
	pointers := map[string]interface{}{}

	for _, kv := range kvv.Versions {
		var doc map[string]interface{}
		// Extract the hash (first byte is the Flag)
		// XXX(tsileo): add/handle a `Deleted` flag
		hash := kv.HexHash()
		// kv.Value[1:len(kv.Value)]

		// Fetch the blob
		blob, err := docstore.blobStore.Get(context.TODO(), hash)
		if err != nil {
			return nil, nil, cursor, fmt.Errorf("failed to fetch blob %v", hash)
		}

		// Build the doc
		if err := msgpack.Unmarshal(blob, &doc); err != nil {
			return nil, nil, cursor, fmt.Errorf("failed to unmarshal blob: %s", blob)
		}
		_id, err := id.FromHex(sid)
		if err != nil {
			panic(err)
		}
		_id.SetHash(kv.HexHash())
		_id.SetVersion(kv.Version)
		addSpecialFields(doc, _id)

		if fetchPointers {
			docPointers, err := docstore.fetchPointers(doc)
			if err != nil {
				return nil, nil, cursor, err
			}
			for k, v := range docPointers {
				pointers[k] = v
			}
		}

		docs = append(docs, doc)
		cursor = kv.Version - 1
		// _id.SetHash(hash)
		// _id.SetFlag(byte(kv.Data[0]))
		// _id.SetVersion(kv.Version)

	}
	return docs, pointers, cursor, nil
}

// Fetch a single document into `res` and returns the `id.ID`
func (docstore *DocStore) Fetch(collection, sid string, res interface{}, fetchPointers bool, version int) (*id.ID, map[string]interface{}, error) {
	if collection == "" {
		return nil, nil, errors.New("missing collection query arg")
	}

	// Fetch the VKV entry for this _id
	kv, err := docstore.kvStore.Get(context.TODO(), fmt.Sprintf(KeyFmt, collection, sid), version)
	if err != nil {
		return nil, nil, err
	}

	// Parse the ID
	_id, err := id.FromHex(sid)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid _id: %v", err)
	}

	// Extract the hash (first byte is the Flag)
	// XXX(tsileo): add/handle a `Deleted` flag
	hash := kv.HexHash()
	// kv.Value[1:len(kv.Value)]

	// Fetch the blob
	blob, err := docstore.blobStore.Get(context.TODO(), hash)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to fetch blob %v", hash)
	}

	var pointers map[string]interface{}

	// Build the doc
	switch idoc := res.(type) {
	case nil:
		// Do nothing
	case *map[string]interface{}:
		if err := msgpack.Unmarshal(blob, idoc); err != nil {
			return nil, nil, fmt.Errorf("failed to unmarshal blob: %s", blob)
		}
		// TODO(tsileo): set the special fields _created/_updated/_hash
		if fetchPointers {
			pointers, err = docstore.fetchPointers(*idoc)
			if err != nil {
				return nil, nil, err
			}
		}
	case *[]byte:
		// Decode the doc and encode it to JSON
		out := map[string]interface{}{}
		if err := msgpack.Unmarshal(blob, &out); err != nil {
			return nil, nil, fmt.Errorf("failed to unmarshal blob: %s", blob)
		}
		// TODO(tsileo): set the special fields _created/_updated/_hash
		js, err := json.Marshal(out)
		if err != nil {
			return nil, nil, err
		}

		// Just the copy if JSON if a []byte is provided
		*idoc = append(*idoc, js...)
	}
	_id.SetHash(hash)
	_id.SetFlag(kv.Data[0])
	_id.SetVersion(kv.Version)
	return _id, pointers, nil
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
			// js := []byte{}
			var doc, pointers map[string]interface{}

			if _id, pointers, err = docstore.Fetch(collection, sid, &doc, true, -1); err != nil {
				if err == vkv.ErrNotFound || _id.Flag() == FlagDeleted {
					// Document doesn't exist, returns a status 404
					w.WriteHeader(http.StatusNotFound)
					return
				}
				panic(err)
			}

			// FIXME(tsileo): fix-precondition, suport If-Match
			if hash := r.Header.Get("If-None-Match"); hash != "" {
				if hash == _id.Hash() {
					w.WriteHeader(http.StatusNotModified)
					return
				}
			}

			w.Header().Set("Etag", _id.Hash())
			addSpecialFields(doc, _id)

			js, err := json.Marshal(map[string]interface{}{
				"data":     doc,
				"pointers": pointers,
			})
			if err != nil {
				panic(err)
			}

			if r.Method == "GET" {
				w.Header().Set("Content-Type", "application/json")
				srw.Write(js)
			}
		case "PATCH":
			// Patch the document (JSON-Patch/RFC6902)

			// Lock the document before making any change to it, this way the PATCH operation is *truly* atomic/safe
			docstore.locker.Lock(sid)
			defer docstore.locker.Unlock(sid)

			ctx := context.Background()

			// Fetch the current doc
			js := []byte{}
			if _id, _, err = docstore.Fetch(collection, sid, &js, false, -1); err != nil {
				if err == vkv.ErrNotFound {
					// Document doesn't exist, returns a status 404
					w.WriteHeader(http.StatusNotFound)
					return
				}
				panic(err)
			}

			// FIXME(tsileo): make it required?
			if hash := r.Header.Get("If-Match"); hash != "" {
				if _id.Hash() != hash {
					w.WriteHeader(http.StatusPreconditionFailed)
					return
				}
			}

			buf, err := ioutil.ReadAll(r.Body)
			if err != nil {
				panic(err)
			}

			patch, err := jsonpatch.DecodePatch(buf)
			if err != nil {
				panic(err)
			}
			docstore.logger.Debug("patch decoded", "patch", patch)

			pdata, err := patch.Apply(js)
			if err != nil {
				panic(err)
			}

			// Back to msgpack
			ndoc := map[string]interface{}{}
			if err := json.Unmarshal(pdata, &ndoc); err != nil {
				panic(err)
			}
			data, err := msgpack.Marshal(ndoc)

			// TODO(tsileo): also check for reserved keys here

			// Compute the Blake2B hash and save the blob
			hash := fmt.Sprintf("%x", blake2b.Sum256(data))
			blob := &blob.Blob{Hash: hash, Data: data}
			if err := docstore.blobStore.Put(ctx, blob); err != nil {
				panic(err)
			}

			if _, err := docstore.kvStore.Put(ctx, fmt.Sprintf(KeyFmt, collection, _id.String()), hash, []byte{_id.Flag()}, -1); err != nil {
				panic(err)
			}

			w.Header().Set("Etag", _id.Hash())
			w.WriteHeader(http.StatusOK)
			srw := httputil.NewSnappyResponseWriter(w, r)

			created := time.Unix(0, _id.Ts()).UTC().Format(time.RFC3339)

			httputil.WriteJSON(srw, map[string]interface{}{
				"_id":      _id.String(),
				"_created": created,
				"_hash":    _id.Hash(),
			})
			srw.Close()

			return
		case "POST":
			// Update the whole document

			// Lock the document before making any change to it
			docstore.locker.Lock(sid)
			defer docstore.locker.Unlock(sid)

			// permissions.CheckPerms(r, PermCollectionName, collection, PermWrite)
			ctx := context.Background()
			// Fetch the actual doc
			doc := map[string]interface{}{}
			_id, _, err = docstore.Fetch(collection, sid, &doc, false, -1)
			if err != nil {
				if err == vkv.ErrNotFound || _id.Flag() == FlagDeleted {
					// Document doesn't exist, returns a status 404
					w.WriteHeader(http.StatusNotFound)
					return
				}
				panic(err)
			}

			// If-Match is optional for POST request
			if hash := r.Header.Get("If-Match"); hash != "" {
				if _id.Hash() != hash {
					w.WriteHeader(http.StatusPreconditionFailed)
					return
				}
			}

			data, err := ioutil.ReadAll(r.Body)
			if err != nil {
				panic(err)
			}

			// Parse the update query
			var newDoc map[string]interface{}
			if err := json.Unmarshal(data, &newDoc); err != nil {
				panic(err)
			}

			// Field/key starting with `_` are forbidden, remove them
			for k, _ := range newDoc {
				if _, ok := reservedKeys[k]; ok {
					delete(newDoc, k)
				}
			}

			data, err = msgpack.Marshal(newDoc)
			if err != nil {
				panic(err)
			}

			docstore.logger.Debug("Update", "_id", sid, "new_doc", newDoc)

			// Compute the Blake2B hash and save the blob
			hash := fmt.Sprintf("%x", blake2b.Sum256(data))
			blob := &blob.Blob{Hash: hash, Data: data}
			if err := docstore.blobStore.Put(ctx, blob); err != nil {
				panic(err)
			}

			if _, err := docstore.kvStore.Put(ctx, fmt.Sprintf(KeyFmt, collection, _id.String()), hash, []byte{_id.Flag()}, -1); err != nil {
				panic(err)
			}
			w.Header().Set("Etag", _id.Hash())
			w.WriteHeader(http.StatusOK)
			srw := httputil.NewSnappyResponseWriter(w, r)

			created := time.Unix(0, _id.Ts()).UTC().Format(time.RFC3339)

			httputil.WriteJSON(srw, map[string]interface{}{
				"_id":      _id.String(),
				"_created": created,
				"_hash":    _id.Hash(),
			})
			srw.Close()

			return
		case "DELETE":
			docstore.locker.Lock(sid)
			defer docstore.locker.Unlock(sid)

			_id, _, err := docstore.Fetch(collection, sid, nil, false, -1)
			if err != nil {
				if err == vkv.ErrNotFound || _id.Flag() == FlagDeleted {
					// Document doesn't exist, returns a status 404
					w.WriteHeader(http.StatusNotFound)
					return
				}
				panic(err)
			}

			// FIXME(tsileo): empty the key, and hanlde it in the get/query
			if _, err := docstore.kvStore.Put(context.TODO(), fmt.Sprintf(KeyFmt, collection, sid), "", []byte{FlagDeleted}, -1); err != nil {
				panic(err)
			}

			// TODO(tsileo): handle index deletion for the given document
			return

		}

		w.Header().Set("BlobStash-DocStore-Doc-Id", sid)
		w.Header().Set("BlobStash-DocStore-Doc-Hash", _id.Hash())
		w.Header().Set("BlobStash-DocStore-Doc-CreatedAt", strconv.FormatInt(_id.Ts(), 10))
	}
}

// HTTP handler for serving/updating a single doc
func (docstore *DocStore) docVersionsHandler() func(http.ResponseWriter, *http.Request) {
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
		srw := httputil.NewSnappyResponseWriter(w, r)
		defer srw.Close()
		switch r.Method {
		case "GET", "HEAD":
			q := httputil.NewQuery(r.URL.Query())
			limit, err := q.GetIntDefault("limit", 50)
			if err != nil {
				httputil.Error(w, err)
				return
			}
			cursor, err := q.GetIntDefault("cursor", int(time.Now().UTC().UnixNano()))
			if err != nil {
				httputil.Error(w, err)
				return
			}
			fetchPointers, err := q.GetBoolDefault("fetch_pointers", true)
			if err != nil {
				httputil.Error(w, err)
				return
			}

			// Serve the document JSON encoded
			// permissions.CheckPerms(r, PermCollectionName, collection, PermRead)
			// js := []byte{}

			docs, pointers, cursor, err := docstore.FetchVersions(collection, sid, cursor, limit, fetchPointers)
			if err != nil {
				if err == vkv.ErrNotFound || _id.Flag() == FlagDeleted {
					// Document doesn't exist, returns a status 404
					w.WriteHeader(http.StatusNotFound)
					return
				}
				panic(err)
			}

			js, err := json.Marshal(map[string]interface{}{
				"pointers": pointers,
				"data":     docs,
				"pagination": map[string]interface{}{
					"cursor":   cursor,
					"has_more": len(docs) == limit,
					"count":    len(docs),
					"per_page": limit,
				},
			})
			if err != nil {
				panic(err)
			}

			if r.Method == "GET" {
				w.Header().Set("Content-Type", "application/json")
				srw.Write(js)
			}
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)

		}
	}
}
