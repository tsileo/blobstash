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
	"os"
	_ "path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/blevesearch/bleve"
	"github.com/dchest/blake2b"
	"github.com/gorilla/mux"
	log "github.com/inconshreveable/log15"
	logext "github.com/inconshreveable/log15/ext"

	_ "github.com/tsileo/blobstash/config/pathutil"
	_ "github.com/tsileo/blobstash/permissions"
	"github.com/tsileo/blobstash/pkg/blob"
	"github.com/tsileo/blobstash/pkg/blobstore"
	"github.com/tsileo/blobstash/pkg/config"
	"github.com/tsileo/blobstash/pkg/ctxutil"
	"github.com/tsileo/blobstash/pkg/docstore/id"
	_ "github.com/tsileo/blobstash/pkg/docstore/index"
	_ "github.com/tsileo/blobstash/pkg/docstore/optimizer"
	"github.com/tsileo/blobstash/pkg/httputil"
	"github.com/tsileo/blobstash/pkg/kvstore"
	"github.com/tsileo/blobstash/vkv"
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

	PermName           = "docstore"
	PermCollectionName = "docstore:collection"
	PermWrite          = "write"
	PermRead           = "read"
)

func hashFromKey(col, key string) string {
	return strings.Replace(key, fmt.Sprintf("docstore:%s:", col), "", 1)
}

const (
	FlagNoIndex         byte = iota // Won't be indexed by Bleve
	FlagFullTextIndexed             // TODO(tsileo): the full text index shouldn't be managed via a Flag, but like regular indexes (HTTP API)
	FlagDeleted
)

type executionStats struct {
	NReturned           int    `json:"nReturned"`
	TotalDocsExamined   int    `json:"totalDocsExamined"`
	ExecutionTimeMillis int    `json:"executionTimeMillis"`
	LastID              string `json:"-"`
	Optimizer           string `json:"optimizer"`
	Index               string `json:"index"`
}

func openIndex(path string) (bleve.Index, error) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		mapping := bleve.NewIndexMapping()
		return bleve.New(path, mapping)
	}
	return bleve.Open(path)
}

// TODO(tsileo): rename to `DocStore`
type DocStoreExt struct {
	kvStore   *kvstore.KvStore
	blobStore *blobstore.BlobStore

	conf *config.Config
	// index    bleve.Index
	// docIndex *index.HashIndexes

	logger log.Logger
}

// New initializes the `DocStoreExt`
func New(logger log.Logger, conf *config.Config, kvStore *kvstore.KvStore, blobStore *blobstore.BlobStore) (*DocStoreExt, error) {
	logger.Debug("init")
	// FIXME(tsileo): add a way to disable the full-text index via the config file

	// Try to load the docstore index (powered by a kv file)
	// docIndex, err := index.New()
	// if err != nil {
	// 	return nil, fmt.Errorf("failed to open the docstore index: %v", err)
	// }
	// indexPath := filepath.Join(pathutil.VarDir(), "docstore.bleve")
	// index, err := openIndex(indexPath)
	// if err != nil {
	// 	return nil, fmt.Errorf("failed to open the Bleve index (for full-text index): %v", err)
	// }
	return &DocStoreExt{
		kvStore:   kvStore,
		blobStore: blobStore,
		conf:      conf,
		// index:     index,
		// docIndex:  docIndex,
		logger: logger,
	}, nil
}

// Close closes all the open DB files.
func (docstore *DocStoreExt) Close() error {
	// if err := docstore.docIndex.Close(); err != nil {
	// 	return err
	// }
	// return docstore.index.Close()
	return nil
}

// RegisterRoute registers all the HTTP handlers for the extension
func (docstore *DocStoreExt) Register(r *mux.Router, basicAuth func(http.Handler) http.Handler) {
	r.Handle("/", basicAuth(http.HandlerFunc(docstore.collectionsHandler())))
	r.Handle("/{collection}", basicAuth(http.HandlerFunc(docstore.docsHandler())))
	// r.Handle("/{collection}/search", middlewares.Auth(http.HandlerFunc(docstore.searchHandler())))
	// r.Handle("/{collection}/_indexes", middlewares.Auth(http.HandlerFunc(docstore.indexesHandler())))
	r.Handle("/{collection}/{_id}", basicAuth(http.HandlerFunc(docstore.docHandler())))
}

// Expand a doc keys (fetch the blob as JSON, or a filesystem reference)
func (docstore *DocStoreExt) expandKeys(doc map[string]interface{}) error {
	for k, v := range doc {
		switch {
		case strings.HasPrefix(k, "@ref/json"):
			// XXX(tsileo): here and at other place, add a util func in hashutil to detect invalid string length at least
			if _, ok := v.(string); !ok {
				return fmt.Errorf("\"@ref/<type>\" exapandable attributes must be a string")
			}
			blob, err := docstore.blobStore.Get(context.TODO(), v.(string))
			if err != nil {
				return fmt.Errorf("failed to expend JSON ref: \"@ref/json => %v\": %v", v, err)
			}
			expanded := map[string]interface{}{}
			if err := json.Unmarshal(blob, &expanded); err != nil {
				return fmt.Errorf("failed to unmarshal expanded blob  \"@ref/json => %v\": %v", v, err)
			}
			// FIXME(tsileo): how to define expandable keys?
			// quick fix: only uses @ref for now?
			// {"key": "@ref/blob/json/<hash>"}
			// {"key": "@ref/json:<hash>"}
			// {"key": "ref://blob:json@<hash>"}
			// {"file": "@blobstash:<hash>"}
			// {"key": ""}
		}
	}
	return nil
}

// Search performs a full-text search on the given `collection`, expects a Bleve query string.
// func (docstore *DocStoreExt) Search(collection, queryString string) ([]byte, *bleve.SearchResult, error) {
// 	// We build the JSON response using a `[]byte`
// 	js := []byte("[")
// 	query := bleve.NewQueryStringQuery(queryString)
// 	searchRequest := bleve.NewSearchRequest(query)
// 	searchResult, err := docstore.index.Search(searchRequest)
// 	if err != nil {
// 		return nil, nil, err
// 	}
// 	for index, sr := range searchResult.Hits {
// 		// Get the document as a raw bytes to prevent useless marsharlling/unmarshalling
// 		jsPart := []byte{}
// 		_, err := docstore.Fetch(collection, sr.ID, &jsPart)
// 		if err != nil {
// 			return nil, nil, err
// 		}
// 		js = append(js, addID(jsPart, sr.ID)...)
// 		if index != len(searchResult.Hits)-1 {
// 			js = append(js, []byte(",")...)
// 		}
// 	}
// 	// Remove the last comma if there's more than one result
// 	if len(searchResult.Hits) > 1 {
// 		js = js[0 : len(js)-1]
// 	}
// 	js = append(js, []byte("]")...)

// 	return js, searchResult, nil
// }

// HTTP handler for the full-text search
// func (docstore *DocStoreExt) searchHandler() func(http.ResponseWriter, *http.Request) {
// 	return func(w http.ResponseWriter, r *http.Request) {
// 		// XXX(tsileo): handle pagination!

// 		vars := mux.Vars(r)

// 		collection := vars["collection"]
// 		if collection == "" {
// 			httputil.WriteJSONError(w, http.StatusInternalServerError, "Missing collection in the URL")
// 			return
// 		}

// 		if r.Method != "GET" && r.Method != "HEAD" {
// 			w.WriteHeader(http.StatusMethodNotAllowed)
// 			return
// 		}

// 		permissions.CheckPerms(r, PermCollectionName, collection, PermRead)

// 		// Actually performs the search
// 		js, sr, err := docstore.Search(collection, r.URL.Query().Get("q"))
// 		if err != nil {
// 			panic(err)
// 		}

// 		// Add some debug headers for the client to be able to inspect what's going on
// 		w.Header().Set("Content-Type", "application/json")
// 		w.Header().Set("BlobStash-DocStore-Query-Returned", strconv.Itoa(int(sr.Total)))
// 		w.Header().Set("BlobStash-DocStore-Query-Exec-Time", strconv.Itoa(int(sr.Took.Nanoseconds()*1e6)))
// 		w.Header().Set("BlobStash-DocStore-Query-Index", "FULL-TEXT")
// 		w.Header().Set("BlobStash-DocStore-Results-Count", strconv.Itoa(int(sr.Total)))

// 		// A HEAD request may be used to check if there's any results or if the count changed.
// 		if r.Method == "HEAD" {
// 			return
// 		}

// 		srw := httputil.NewSnappyResponseWriter(w, r)
// 		srw.Write(js)
// 		srw.Close()
// 	}
// }

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
func (docstore *DocStoreExt) Collections() ([]string, error) {
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
func (docstore *DocStoreExt) collectionsHandler() func(http.ResponseWriter, *http.Request) {
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
	if q == "" || q == "{}" {
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
func (docstore *DocStoreExt) Insert(collection string, idoc interface{}, ns string, index bool) (*id.ID, error) {
	switch doc := idoc.(type) {
	case *map[string]interface{}:
		docFlag := FlagNoIndex
		if index {
			docFlag = FlagFullTextIndexed
		}
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

		ctx := ctxutil.WithNamespace(context.Background(), ns)

		blob := &blob.Blob{Hash: hash, Data: data}
		if err := docstore.blobStore.Put(ctx, blob); err != nil {
			return nil, err
		}

		// Build the ID and add some meta data
		now := time.Now().UTC().Unix()
		_id, err := id.New(int(now))
		if err != nil {
			return nil, err
		}
		_id.SetHash(hash)
		_id.SetFlag(docFlag)

		// Append the Flag to the KV value
		bash := make([]byte, len(hash)+1)
		bash[0] = docFlag
		copy(bash[1:], []byte(hash)[:])

		// Create a pointer in the key-value store
		if _, err := docstore.kvStore.PutPrefix(ctx, fmt.Sprintf(PrefixKeyFmt, collection), _id.String(), string(bash), -1); err != nil {
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

// Query performs a MongoDB like query on the given collection
func (docstore *DocStoreExt) Query(collection string, query map[string]interface{}, cursor string, limit int, res interface{}) (*executionStats, error) {
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
func (docstore *DocStoreExt) query(collection string, query map[string]interface{}, cursor string, limit int) ([]byte, *executionStats, error) {
	js := []byte("[")
	tstart := time.Now()
	stats := &executionStats{}

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
	var noQuery bool
	fetchLimit := limit
	if query != nil && len(query) > 0 {
		// Prefetch more docs since there's a lot of chance the query won't
		// match every documents
		fetchLimit = int(float64(limit) * 1.3)
	} else {
		noQuery = true
	}

	qLogger := docstore.logger.New("query", query, "id", logext.RandId(8))
	qLogger.Info("new query")

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
		_ids := []string{}
		// switch optzType {
		// case optimizer.Linear:
		// Performs a unoptimized linear scan
		res, err := docstore.kvStore.ReversePrefixKeys(fmt.Sprintf(PrefixKeyFmt, collection), start, end, fetchLimit)
		if err != nil {
			panic(err)
		}
		for _, kv := range res {
			_ids = append(_ids, hashFromKey(collection, kv.Key))
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

		for _, _id := range _ids {
			// Check if the doc match the query
			jsPart := []byte{}
			qLogger.Debug("fetch doc", "_id", _id)
			if _, err := docstore.Fetch(collection, _id, &jsPart); err != nil {
				if err == vkv.ErrNotFound {
					break
				}
				panic(err)
			}
			// FIXME(tsileo): unmarshal all the doc for expanding keys
			stats.TotalDocsExamined++
			if noQuery {
				// No query, so we just add every docs
				js = append(js, addID(jsPart, _id)...)
				js = append(js, []byte(",")...)
				stats.NReturned++
				stats.LastID = _id
				if stats.NReturned == limit {
					break QUERY
				}
			} else {
				doc := map[string]interface{}{}
				if err := json.Unmarshal(jsPart, &doc); err != nil {
					panic(err)
				}
				if matchQuery(qLogger, query, doc) {
					js = append(js, addID(jsPart, _id)...)
					js = append(js, []byte(",")...)
					stats.NReturned++
					stats.LastID = _id
					if stats.NReturned == limit {
						break QUERY
					}
				}
			}
		}
		if len(_ids) == 0 || len(_ids) < limit {
			break
		}
		start = nextKey(lastKey)
	}
	// Remove the last comma fron the JSON string
	if stats.NReturned > 0 {
		js = js[0 : len(js)-1]
	}
	js = append(js, []byte("]")...)

	duration := time.Since(tstart)
	qLogger.Debug("scan done", "duration", duration, "nReturned", stats.NReturned, "scanned", stats.TotalDocsExamined)
	// XXX(tsileo): display duration in string format or nanosecond precision??
	stats.ExecutionTimeMillis = int(duration.Nanoseconds() / 1e6)
	return js, stats, nil
}

// HTTP handler for the collection (handle listing+query+insert)
func (docstore *DocStoreExt) docsHandler() func(http.ResponseWriter, *http.Request) {
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
			query := map[string]interface{}{}
			jsQuery := q.Get("query")
			if jsQuery != "" {
				if err := json.Unmarshal([]byte(jsQuery), &query); err != nil {
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
			js, stats, err := docstore.query(collection, query, cursor, limit)
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
			index := false
			if indexHeader := r.Header.Get("BlobStash-DocStore-IndexFullText"); indexHeader != "" {
				if shouldIndex, _ := strconv.ParseBool(indexHeader); shouldIndex {
					index = true
				}

			}
			ns := r.Header.Get("BlobStash-Namespace")

			// Actually insert the doc
			_id, err := docstore.Insert(collection, &doc, ns, index)
			if err != nil {
				panic(err)
			}

			// Output some headers
			w.Header().Set("BlobStash-DocStore-Doc-Id", _id.String())
			w.Header().Set("BlobStash-DocStore-Doc-Hash", _id.Hash())
			w.Header().Set("BlobStash-DocStore-Doc-CreatedAt", strconv.Itoa(_id.Ts()))
			w.WriteHeader(http.StatusCreated)
			srw := httputil.NewSnappyResponseWriter(w, r)
			httputil.WriteJSON(srw, map[string]interface{}{"_id": _id.String()})
			srw.Close()

		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}
}

// Fetch a single document into `res` and returns the `id.ID`
func (docstore *DocStoreExt) Fetch(collection, sid string, res interface{}) (*id.ID, error) {
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
	hash := kv.Value[1:len(kv.Value)]

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
	case *[]byte:
		// Just the copy if JSON if a []byte is provided
		*idoc = append(*idoc, blob...)
	}
	_id.SetHash(hash)
	_id.SetFlag(byte(kv.Value[0]))
	return _id, nil
}

// HTTP handler for serving/updating a single doc
func (docstore *DocStoreExt) docHandler() func(http.ResponseWriter, *http.Request) {
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

			// Preprend the doc Flag for the VKV pointer
			bash := make([]byte, len(hash)+1)
			// XXX(tsileo): allow to update the flag?
			bash[0] = _id.Flag()
			copy(bash[1:], []byte(hash)[:])

			if _, err := docstore.kvStore.Put(ctx, fmt.Sprintf(KeyFmt, collection, _id.String()), string(bash), -1); err != nil {
				panic(err)
			}

			httputil.WriteJSON(srw, newDoc)
		case "DELETE":
			// FIXME(tsileo): empty the key, and hanlde it in the get/query
		}

		w.Header().Set("BlobStash-DocStore-Doc-Id", sid)
		w.Header().Set("BlobStash-DocStore-Doc-Hash", _id.Hash())
		w.Header().Set("BlobStash-DocStore-Doc-CreatedAt", strconv.Itoa(_id.Ts()))
	}
}
