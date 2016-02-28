/*

Package docstore implements a JSON-based document store
built on top of the Versioned Key-Value store and the Blob store.

Each document will get assigned a MongoDB like ObjectId:

	<binary encoded uint32 (4 bytes) + blob ref (32 bytes)>

The resulting id will have a length of 72 characters encoded as hex.

The JSON document will be stored as is and kvk entry will reference it.

	docstore:<collection>:<id> => <flag (1 byte)>

The pointer just contains a one byte flag as value since the hash is contained in the id.

Document will be automatically sorted by creation time thanks to the ID.

The raw JSON will be store unmodified but the API will add these fields on the fly:

 - `_id`: the hex ID
 - `_hash`: the hash of the JSON blob
 - `_created_at`: UNIX timestamp of creation date

*/
package docstore

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/blevesearch/bleve"
	"github.com/dchest/blake2b"
	"github.com/gorilla/mux"
	log "gopkg.in/inconshreveable/log15.v2"
	logext "gopkg.in/inconshreveable/log15.v2/ext"

	"github.com/tsileo/blobstash/config/pathutil"
	"github.com/tsileo/blobstash/embed"
	"github.com/tsileo/blobstash/ext/docstore/id"
	"github.com/tsileo/blobstash/ext/docstore/index"
	"github.com/tsileo/blobstash/ext/docstore/optimizer"
	"github.com/tsileo/blobstash/httputil"
	serverMiddleware "github.com/tsileo/blobstash/middleware"
	"github.com/tsileo/blobstash/vkv"
)

var (
	PrefixKeyFmt = "docstore:%s"
	KeyFmt       = PrefixKeyFmt + ":%s"

	PrefixIndexKeyFmt = "docstore-index:%s"
	IndexKeyFmt       = PrefixIndexKeyFmt + ":%s"
)

func hashFromKey(col, key string) string {
	return strings.Replace(key, fmt.Sprintf("docstore:%s:", col), "", 1)
}

const (
	FlagNoIndex byte = iota // Won't be indexed by Bleve
	FlagIndexed
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

// TODO(tsileo): full text indexing, find a way to get the config index

func openIndex(path string) (bleve.Index, error) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		mapping := bleve.NewIndexMapping()
		return bleve.New(path, mapping)
	}
	return bleve.Open(path)
}

type DocStoreExt struct {
	kvStore   *embed.KvStore
	blobStore *embed.BlobStore

	index    bleve.Index
	docIndex *index.HashIndexes

	logger log.Logger
}

func New(logger log.Logger, kvStore *embed.KvStore, blobStore *embed.BlobStore) *DocStoreExt {
	indexPath := filepath.Join(pathutil.VarDir(), "docstore.bleve")
	docIndex, err := index.New()
	if err != nil {
		panic(err)
	}
	index, err := openIndex(indexPath)
	if err != nil {
		// TODO(tsileo): returns an error instead
		panic(err)
	}
	logger.Debug("Bleve index init", "index-path", indexPath)
	return &DocStoreExt{
		kvStore:   kvStore,
		blobStore: blobStore,
		index:     index,
		docIndex:  docIndex,
		logger:    logger,
	}
}

func (docstore *DocStoreExt) Close() error {
	if err := docstore.docIndex.Close(); err != nil {
		return err
	}
	return docstore.index.Close()
}

func (docstore *DocStoreExt) RegisterRoute(r *mux.Router, middlewares *serverMiddleware.SharedMiddleware) {
	r.Handle("/", middlewares.Auth(http.HandlerFunc(docstore.collectionsHandler())))
	r.Handle("/{collection}", middlewares.Auth(http.HandlerFunc(docstore.docsHandler())))
	r.Handle("/{collection}/search", middlewares.Auth(http.HandlerFunc(docstore.searchHandler())))
	r.Handle("/{collection}/_indexes", middlewares.Auth(http.HandlerFunc(docstore.indexesHandler())))
	r.Handle("/{collection}/{_id}", middlewares.Auth(http.HandlerFunc(docstore.docHandler())))
}

func (docstore *DocStoreExt) Search(collection, queryString string) ([]byte, *bleve.SearchResult, error) {
	// We build the JSON response using a `[]byte`
	js := []byte("[")
	query := bleve.NewQueryStringQuery(queryString)
	searchRequest := bleve.NewSearchRequest(query)
	searchResult, err := docstore.index.Search(searchRequest)
	if err != nil {
		return nil, nil, err
	}
	for index, sr := range searchResult.Hits {
		jsPart := []byte{}
		_, err := docstore.Fetch(collection, sr.ID, &jsPart)
		if err != nil {
			return nil, nil, err
		}
		js = append(js, addID(jsPart, sr.ID)...)
		if index != len(searchResult.Hits)-1 {
			js = append(js, []byte(",")...)
		}
	}
	// Remove the last comma if there's more than one result
	if len(searchResult.Hits) > 1 {
		js = js[0 : len(js)-1]
	}
	js = append(js, []byte("]")...)

	return js, searchResult, nil
}

func (docstore *DocStoreExt) searchHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		collection := vars["collection"]
		if collection == "" {
			panic("missing collection query arg")
		}
		js, sr, err := docstore.Search(collection, r.URL.Query().Get("q"))
		if err != nil {
			panic(err)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("BlobStash-DocStore-Query-Returned", strconv.Itoa(int(sr.Total)))
		w.Header().Set("BlobStash-DocStore-Query-Exec-Time", strconv.Itoa(int(sr.Took.Nanoseconds()/1e6)))

		srw := httputil.NewSnappyResponseWriter(w, r)
		srw.Write(js)
		srw.Close()
	}
}

// nextKey returns the next key for lexigraphical (key = nextKey(lastkey))
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

func (docstore *DocStoreExt) Collections() ([]string, error) {
	collections := []string{}
	lastKey := ""
	for {
		ksearch := fmt.Sprintf("docstore:%v", lastKey)
		res, err := docstore.kvStore.Keys(ksearch, "\xff", 50)
		// docstore.logger.Debug("loop", "ksearch", ksearch, "len_res", len(res))
		if err != nil {
			return nil, err
		}
		if len(res) == 0 {
			break
		}
		var col string
		for _, kv := range res {
			col = strings.Split(kv.Key, ":")[1]
			collections = append(collections, col)
		}
		lastKey = nextKey(col)
	}
	return collections, nil
}

func (docstore *DocStoreExt) indexesHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		collection := vars["collection"]
		if collection == "" {
			panic("missing collection query arg")
		}
		switch r.Method {
		case "GET":
			// q := r.URL.Query()
			srw := httputil.NewSnappyResponseWriter(w, r)
			indexes, err := docstore.Indexes(collection)
			if err != nil {
				panic(err)
			}
			httputil.WriteJSON(srw, indexes)
			srw.Close()
		case "POST":
			q := map[string]interface{}{}
			if err := json.NewDecoder(r.Body).Decode(&q); err != nil {
				panic(err)
			}
			if err := docstore.AddIndex(collection, q); err != nil {
				panic(err)
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}
}

func (docstore *DocStoreExt) collectionsHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "GET":
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

func (docstore *DocStoreExt) Indexes(collection string) ([]*index.Index, error) {
	res, err := docstore.kvStore.ReversePrefixKeys(fmt.Sprintf(PrefixIndexKeyFmt, collection), "", "\xff", 50)
	indexes := []*index.Index{}
	if err != nil {
		panic(err)
	}
	for _, kv := range res {
		index := &index.Index{ID: strings.Replace(kv.Key, fmt.Sprintf(IndexKeyFmt, collection, ""), "", 1)}
		if err := json.Unmarshal([]byte(kv.Value), index); err != nil {
			return nil, err
		}
		indexes = append(indexes, index)
	}
	return indexes, nil
}

func (docstore *DocStoreExt) AddIndex(collection string, q map[string]interface{}) error {
	// FIXME(tsileo): handle object with more kvs
	if len(q) > 1 {
		return fmt.Errorf("Indexed query must be of lengh 1 for now")
	}
	fields := []string{}
	for _, ifield := range q["fields"].([]interface{}) {
		fields = append(fields, ifield.(string))
	}
	js, err := json.Marshal(q)
	if err != nil {
		return err
	}
	hashKey := fmt.Sprintf("single-field-%s", fields[0])
	_, err = docstore.kvStore.PutPrefix(fmt.Sprintf(PrefixIndexKeyFmt, collection), hashKey, string(js), -1, "")
	return err
}

func (docstore *DocStoreExt) Insert(collection string, idoc interface{}, ns string, index bool) (*id.ID, error) {
	switch doc := idoc.(type) {
	case *map[string]interface{}:
		// fdoc := *doc
		docFlag := FlagNoIndex
		if index {
			docFlag = FlagIndexed
		}
		blob, err := json.Marshal(doc)
		if err != nil {
			return nil, err
		}
		// FIXME(tsileo): What to do if there's an empty ID?

		// Store the payload (JSON data) in a blob
		hash := fmt.Sprintf("%x", blake2b.Sum256(blob))
		docstore.blobStore.Put(hash, blob, ns)
		// Create a pointer in the key-value store
		now := time.Now().UTC().Unix()
		_id, err := id.New(int(now))
		if err != nil {
			return nil, err
		}
		bash := make([]byte, len(hash)+1)
		bash[0] = docFlag
		copy(bash[1:], []byte(hash)[:])
		if _, err := docstore.kvStore.PutPrefix(fmt.Sprintf(PrefixKeyFmt, collection), _id.String(), string(bash), -1, ns); err != nil {
			return nil, err
		}
		if docFlag == FlagIndexed {
			if err := docstore.index.Index(_id.String(), doc); err != nil {
				return nil, err
			}
		}
		_id.SetHash(hash)

		indexes, err := docstore.Indexes(collection)
		if err != nil {
			return nil, fmt.Errorf("Failed to fetch index")
		}
		optz := optimizer.New(docstore.logger.New("module", "query optimizer"), indexes)
		shouldIndex, idx, idxKey := optz.ShouldIndex(*doc)
		if shouldIndex {
			docstore.logger.Debug("indexing document", "idx-key", idxKey, "_id", _id.String())
			if err := docstore.docIndex.Index(collection, idx, idxKey, _id.String()); err != nil {
				return _id, nil
			}
		}
		// FIXME(tsileo): remove counter from ID
		return _id, nil
	}
	return nil, fmt.Errorf("doc must be a *map[string]interface{}")
}

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
	indexes, err := docstore.Indexes(collection)
	if err != nil {
		return nil, nil, fmt.Errorf("Failed to fetch index")
	}
	optz := optimizer.New(docstore.logger.New("module", "query optimizer"), indexes)
	start := ""
	if cursor != "" {
		start = fmt.Sprintf(KeyFmt, collection, cursor)
	}
	end := "\xff"
	if query != nil && len(query) > 0 {
		// Prefetch more docs since there's a lot of chance the query won't
		// match every documents
		limit = int(float64(limit) * 1.3)
	}
	var noQuery bool
	if query == nil || len(query) == 0 {
		noQuery = true
	}
	qLogger := docstore.logger.New("query", query, "id", logext.RandId(8))
	qLogger.Info("new query")
	optzType, optzIndex := optz.Select(query)
	stats.Optimizer = optzType
	if optzIndex != nil {
		stats.Index = optzIndex.ID
	}
	var lastKey string
	for {
		qLogger.Debug("internal query", "limit", limit, "cursor", cursor, "start", start, "end", end, "nreturned", stats.NReturned, "optimizer", optzType)
		// FIXME(tsileo): use `PrefixKeys` if ?sort=_id (-_id by default).
		_ids := []string{}
		switch optzType {
		case optimizer.Linear:
			res, err := docstore.kvStore.ReversePrefixKeys(fmt.Sprintf(PrefixKeyFmt, collection), start, end, limit) // Prefetch more docs
			if err != nil {
				panic(err)
			}
			for _, kv := range res {
				_ids = append(_ids, hashFromKey(collection, kv.Key))
				lastKey = kv.Key
			}
		case optimizer.Index:
			// FIXME(tsileo): make cursor works (start, end handling)
			optzIndexHash := index.IndexKey(query[optzIndex.Fields[0]])
			_ids, err = docstore.docIndex.Iter(collection, optzIndex, optzIndexHash, start, end, limit)
			if err != nil {
				panic(err)
			}
		}
		for _, _id := range _ids {
			jsPart := []byte{}
			qLogger.Debug("fetch doc", "_id", _id)
			if _, err := docstore.Fetch(collection, _id, &jsPart); err != nil {
				panic(err)
			}
			stats.TotalDocsExamined++
			if noQuery {
				// No query, so we just add every docs
				js = append(js, addID(jsPart, _id)...)
				js = append(js, []byte(",")...)
				stats.NReturned++
				stats.LastID = _id
				if stats.NReturned == limit {
					break
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
						break
					}
				}
			}
		}
		if len(_ids) == 0 || len(_ids) < limit {
			break
		}
		start = nextKey(lastKey)
	}
	if stats.NReturned > 0 {
		js = js[0 : len(js)-1]
	}
	duration := time.Since(tstart)
	qLogger.Debug("scan done", "duration", duration, "nReturned", stats.NReturned, "scanned", stats.TotalDocsExamined)
	stats.ExecutionTimeMillis = int(duration.Nanoseconds() / 1e6)
	js = append(js, []byte("]")...)
	return js, stats, nil
}

func (docstore *DocStoreExt) docsHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		vars := mux.Vars(r)
		collection := vars["collection"]
		if collection == "" {
			panic("missing collection query arg")
		}
		switch r.Method {
		case "GET":
			// Parse the cursor
			cursor := q.Get("cursor")

			// Parse the query (JSON-encoded)
			query := map[string]interface{}{}
			jsQuery := q.Get("query")
			if jsQuery != "" {
				if err := json.Unmarshal([]byte(jsQuery), &query); err != nil {
					panic(err)
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

			w.Header().Set("BlobStash-DocStore-Query-Optimizer", stats.Optimizer)
			if stats.Optimizer != optimizer.Linear {
				w.Header().Set("BlobStash-DocStore-Query-Index", stats.Index)
			}

			// Set headers for the query stats
			w.Header().Set("BlobStash-DocStore-Query-Returned", strconv.Itoa(stats.NReturned))
			w.Header().Set("BlobStash-DocStore-Query-Examined", strconv.Itoa(stats.TotalDocsExamined))
			w.Header().Set("BlobStash-DocStore-Query-Exec-Time", strconv.Itoa(stats.ExecutionTimeMillis))

			// Write the JSON response (encoded if requested)
			w.Header().Set("Content-Type", "application/json")
			srw := httputil.NewSnappyResponseWriter(w, r)
			srw.Write(js)
			srw.Close()
		case "POST":
			// Read the whole body
			blob, err := ioutil.ReadAll(r.Body)
			if err != nil {
				panic(err)
			}
			// Ensure it's JSON encoded
			doc := map[string]interface{}{}
			if err := json.Unmarshal(blob, &doc); err != nil {
				panic(err)
			}
			index := false
			if indexHeader := r.Header.Get("BlobStash-DocStore-IndexFullText"); indexHeader != "" {
				if shouldIndex, _ := strconv.ParseBool(indexHeader); shouldIndex {
					index = true
				}

			}
			ns := r.Header.Get("BlobStash-Namespace")
			_id, err := docstore.Insert(collection, &doc, ns, index)
			if err != nil {
				panic(err)
			}
			w.Header().Set("BlobStash-DocStore-Doc-Id", _id.String())
			w.Header().Set("BlobStash-DocStore-Doc-Hash", _id.Hash())
			w.Header().Set("BlobStash-DocStore-Doc-CreatedAt", strconv.Itoa(_id.Ts()))
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}
}

func (docstore *DocStoreExt) Fetch(collection, sid string, res interface{}) (*id.ID, error) {
	if collection == "" {
		return nil, errors.New("missing collection query arg")
	}
	kv, err := docstore.kvStore.Get(fmt.Sprintf(KeyFmt, collection, sid), -1)
	if err != nil {
		return nil, err
		// fmt.Errorf("kvstore get err: %v", err)
	}
	_id, err := id.FromHex(sid)
	if err != nil {
		return nil, fmt.Errorf("invalid _id: %v", err)
	}
	hash := kv.Value[1:len(kv.Value)]
	// Fetch the blob
	blob, err := docstore.blobStore.Get(hash)
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
	return _id, nil
}

func (docstore *DocStoreExt) docHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		collection := vars["collection"]
		if collection == "" {
			panic("missing collection query arg")
		}
		sid := vars["_id"]
		if sid == "" {
			panic("missing _id query arg")
		}
		var _id *id.ID
		var err error
		srw := httputil.NewSnappyResponseWriter(w, r)
		defer srw.Close()
		switch r.Method {
		case "GET", "HEAD":
			js := []byte{}
			if _id, err = docstore.Fetch(collection, sid, &js); err != nil {
				if err == vkv.ErrNotFound {
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
			ns := r.Header.Get("BlobStash-Namespace")
			doc := map[string]interface{}{}
			if _id, err = docstore.Fetch(collection, sid, &doc); err != nil {
				panic(err)
			}
			var update map[string]interface{}
			decoder := json.NewDecoder(r.Body)
			if err := decoder.Decode(&update); err != nil {
				panic(err)
			}
			docstore.logger.Debug("Update", "_id", sid, "ns", ns, "update_query", update)
			// FIXME(tsileo): nore more $set, move to PATCH and make it acts like $set
			newDoc, err := updateDoc(doc, update)
			blob, err := json.Marshal(newDoc)
			if err != nil {
				panic(err)
			}
			hash := fmt.Sprintf("%x", blake2b.Sum256(blob))
			docstore.blobStore.Put(hash, blob, ns)
			bash := make([]byte, len(hash)+1)
			// FIXME(tsileo): fetch previous flag and set the same
			bash[0] = FlagIndexed
			copy(bash[1:], []byte(hash)[:])
			if _, err := docstore.kvStore.Put(fmt.Sprintf(KeyFmt, collection, _id.String()), string(bash), -1, ns); err != nil {
				panic(err)
			}
			httputil.WriteJSON(srw, newDoc)
		}
		w.Header().Set("BlobStash-DocStore-Doc-Id", sid)
		w.Header().Set("BlobStash-DocStore-Doc-Hash", _id.Hash())
		w.Header().Set("BlobStash-DocStore-Doc-CreatedAt", strconv.Itoa(_id.Ts()))
	}
}
