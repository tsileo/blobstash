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
	"github.com/tsileo/blobstash/client/interface"
	"github.com/tsileo/blobstash/config/pathutil"
	"github.com/tsileo/blobstash/ext/docstore/id"
	serverMiddleware "github.com/tsileo/blobstash/middleware"
	log "gopkg.in/inconshreveable/log15.v2"
	logext "gopkg.in/inconshreveable/log15.v2/ext"
)

var KeyFmt = "docstore:%s:%s"

func hashFromKey(col, key string) string {
	return strings.Replace(key, fmt.Sprintf("docstore:%s:", col), "", 1)
}

const (
	FlagNoIndex byte = iota // Won't be indexed by Bleve
	FlagIndexed
	FlagDeleted
)

type executionStats struct {
	NReturned           int `json:"nReturned"`
	TotalDocsExamined   int `json:"totalDocsExamined"`
	ExecutionTimeMillis int `json:"executionTimeMillis"`
}

// TODO(ts) full text indexing, find a way to get the config index

// FIXME(ts) move this in utils/http
func WriteJSON(w http.ResponseWriter, data interface{}) {
	js, err := json.Marshal(data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(js)
}

func openIndex(path string) (bleve.Index, error) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		mapping := bleve.NewIndexMapping()
		return bleve.New(path, mapping)
	}
	return bleve.Open(path)
}

type DocStoreExt struct {
	kvStore   client.KvStorer
	blobStore client.BlobStorer

	index bleve.Index

	logger log.Logger
}

func New(logger log.Logger, kvStore client.KvStorer, blobStore client.BlobStorer) *DocStoreExt {
	indexPath := filepath.Join(pathutil.VarDir(), "docstore.bleve")
	index, err := openIndex(indexPath)
	if err != nil {
		// TODO(ts) returns an error instead
		panic(err)
	}
	logger.Debug("Bleve index init", "index-path", indexPath)
	return &DocStoreExt{
		kvStore:   kvStore,
		blobStore: blobStore,
		index:     index,
		logger:    logger,
	}
}

func (docstore *DocStoreExt) Close() error {
	return docstore.index.Close()
}

func (docstore *DocStoreExt) RegisterRoute(r *mux.Router, middlewares *serverMiddleware.SharedMiddleware) {
	r.Handle("/", middlewares.Auth(http.HandlerFunc(docstore.collectionsHandler())))
	r.Handle("/{collection}", middlewares.Auth(http.HandlerFunc(docstore.docsHandler())))
	r.Handle("/{collection}/search", middlewares.Auth(http.HandlerFunc(docstore.searchHandler())))
	r.Handle("/{collection}/{_id}", middlewares.Auth(http.HandlerFunc(docstore.docHandler())))
}

func (docstore *DocStoreExt) Search(collection, queryString string) ([]byte, error) {
	js := []byte{}
	query := bleve.NewQueryStringQuery(queryString)
	searchRequest := bleve.NewSearchRequest(query)
	searchResult, err := docstore.index.Search(searchRequest)
	if err != nil {
		return nil, err
	}
	for index, sr := range searchResult.Hits {
		jsPart := []byte{}
		_, err := docstore.fetchDoc(collection, sr.ID, &jsPart)
		if err != nil {
			return nil, err
		}
		js = append(js, addID(jsPart, sr.ID)...)
		if index != len(searchResult.Hits)-1 {
			js = append(js, []byte(",")...)
		}
	}
	if len(searchResult.Hits) > 0 {
		js = js[0 : len(js)-1]
	}
	js = append(js, []byte("]")...)
	// "_meta": searchResult,
	// "data":  docs,
	// TODO(tsileo) returns meta along with argument
	return js, nil
}

func (docstore *DocStoreExt) searchHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		collection := vars["collection"]
		if collection == "" {
			panic("missing collection query arg")
		}
		js, err := docstore.Search(collection, r.URL.Query().Get("q"))
		if err != nil {
			panic(err)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
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

func (docstore *DocStoreExt) collectionsHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "GET":
			collections, err := docstore.Collections()
			if err != nil {
				panic(err)
			}
			WriteJSON(w, map[string]interface{}{
				"collections": collections,
			})
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

func (docstore *DocStoreExt) Insert(collection string, idoc interface{}) (*id.ID, error) {
	switch doc := idoc.(type) {
	case *map[string]interface{}:
		// fdoc := *doc
		docFlag := FlagNoIndex
		// docFlag = FlagIndexed
		blob, err := json.Marshal(doc)
		if err != nil {
			return nil, err
		}
		// Store the payload in a blob
		hash := fmt.Sprintf("%x", blake2b.Sum256(blob))
		docstore.blobStore.Put(hash, blob)
		// Create a pointer in the key-value store
		now := time.Now().UTC().Unix()
		_id, err := id.New(int(now))
		if err != nil {
			return nil, err
		}
		bash := make([]byte, len(hash)+1)
		bash[0] = docFlag
		copy(bash[1:], []byte(hash)[:])
		if _, err := docstore.kvStore.Put(fmt.Sprintf(KeyFmt, collection, _id.String()), string(bash), -1); err != nil {
			return nil, err
		}
		// Returns the doc along with its new ID
		// if err := docstore.index.Index(_id.String(), doc); err != nil {
		// return err
		// }
		_id.SetHash(hash)
		return _id, nil
	}
	return nil, fmt.Errorf("shouldn't happen")
}

func (docstore *DocStoreExt) Query(collection string, query map[string]interface{}, limit int, res interface{}) (*executionStats, error) {
	js, stats, err := docstore.query(collection, query, limit)
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
func (docstore *DocStoreExt) query(collection string, query map[string]interface{}, limit int) ([]byte, *executionStats, error) {
	js := []byte("[")
	tstart := time.Now()
	stats := &executionStats{}
	start := fmt.Sprintf(KeyFmt, collection, "") // q.Get("start"))
	// TODO(ts) check the \xff
	end := fmt.Sprintf(KeyFmt, collection, "\xff") // q.Get("end")+"\xff")
	// FIXME(ts) we may have to scan all the docs for answering the query
	// so this call should be in a for loop
	if query == nil || len(query) == 0 {
		// Prefetch more docs since there's a lot of chance the query will
		// match every documents
		limit = int(float64(limit) * 1.6)
	}
	qLogger := docstore.logger.New("query", query, "id", logext.RandId(8))
	qLogger.Info("new query")
	var lastKey string
	for {
		qLogger.Debug("internal query", "limit", limit, "start", start, "end", end, "nreturned", stats.NReturned)
		res, err := docstore.kvStore.Keys(start, end, limit) // Prefetch more docs
		if err != nil {
			panic(err)
		}
		for _, kv := range res {
			jsPart := []byte{}
			// TODO(tsileo) send the Ids in the header
			// doc["_id"] = _id.String()
			_id := hashFromKey(collection, kv.Key)
			if _, err := docstore.fetchDoc(collection, _id, &jsPart); err != nil {
				panic(err)
			}
			stats.TotalDocsExamined++
			if len(query) == 0 {
				// No query, so we just add every docs
				js = append(js, addID(jsPart, _id)...)
				js = append(js, []byte(",")...)
				stats.NReturned++
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
					if stats.NReturned == limit {
						break
					}
				}
			}
			lastKey = kv.Key
		}
		if len(res) == 0 || len(res) < limit {
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
		// explainMode := false
		// if r.Header.Get("BlobStash-DocStore-Explain-Mode") != "" || q.Get("explain") != "" {
		// 	explainMode = true
		// }
		// FIXME(ts) returns the execution stats and add a debug mode in the CLI
		switch r.Method {
		case "GET":
			query := map[string]interface{}{}
			jsQuery := q.Get("query")
			if jsQuery != "" {
				if err := json.Unmarshal([]byte(jsQuery), &query); err != nil {
					panic(err)
				}
			}
			limit := 50
			if q.Get("limit") != "" {
				ilimit, err := strconv.Atoi(q.Get("limit"))
				if err != nil {
					http.Error(w, "bad limit", 500)
				}
				limit = ilimit
			}
			// FIXME(tsileo) write IDs + stats as headers
			js, _, err := docstore.query(collection, query, limit)
			if err != nil {
				panic(err)
			}
			w.Header().Set("Content-Type", "application/json")
			w.Write(js)
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
			// FIXME(tsileo) re-enable full-text index
			// docFlag := FlagNoIndex
			// // Should the doc be full-text indexed?
			// indexHeader := r.Header.Get("BlobStash-DocStore-IndexFullText")
			// if indexHeader != "" {
			// 	docFlag = FlagIndexed
			// }
			_id, err := docstore.Insert(collection, &doc)
			if err != nil {
				panic(err)
			}
			// if indexHeader != "" {
			// 	if err := docstore.index.Index(_id.String(), doc); err != nil {
			// 		panic(err)
			// 	}
			// }
			w.Header().Set("BlobStash-DocStore-Doc-Id", _id.String())
			w.Header().Set("BlobStash-DocStore-Doc-Hash", _id.Hash())
			w.Header().Set("BlobStash-DocStore-Doc-CreatedAt", strconv.Itoa(_id.Ts()))
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}
}

func (docstore *DocStoreExt) fetchDoc(collection, sid string, res interface{}) (*id.ID, error) {
	if collection == "" {
		return nil, errors.New("missing collection query arg")
	}
	kv, err := docstore.kvStore.Get(fmt.Sprintf(KeyFmt, collection, sid), -1)
	if err != nil {
		return nil, fmt.Errorf("kvstore get err: %v", err)
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
		switch r.Method {
		case "GET":
			js := []byte{}
			if _id, err = docstore.fetchDoc(collection, sid, &js); err != nil {
				panic(err)
			}
			w.Header().Set("Content-Type", "application/json")
			w.Write(addID(js, sid))
		case "POST":
			doc := map[string]interface{}{}
			if _id, err = docstore.fetchDoc(collection, sid, &doc); err != nil {
				panic(err)
			}
			var update map[string]interface{}
			decoder := json.NewDecoder(r.Body)
			if err := decoder.Decode(&update); err != nil {
				panic(err)
			}
			docstore.logger.Debug("Update", "_id", sid, "update_query", update)
			newDoc, err := updateDoc(doc, update)
			blob, err := json.Marshal(newDoc)
			if err != nil {
				panic(err)
			}
			hash := fmt.Sprintf("%x", blake2b.Sum256(blob))
			docstore.blobStore.Put(hash, blob)
			bash := make([]byte, len(hash)+1)
			// FIXME(tsileo) fetch previous flag and set the same
			bash[0] = FlagIndexed
			copy(bash[1:], []byte(hash)[:])
			if _, err := docstore.kvStore.Put(fmt.Sprintf(KeyFmt, collection, _id.String()), string(bash), -1); err != nil {
				panic(err)
			}
			WriteJSON(w, newDoc)
		}
		w.Header().Set("BlobStash-DocStore-Doc-Id", sid)
		w.Header().Set("BlobStash-DocStore-Doc-Hash", _id.Hash())
		w.Header().Set("BlobStash-DocStore-Doc-CreatedAt", strconv.Itoa(_id.Ts()))
	}
}
