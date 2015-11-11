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
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/blevesearch/bleve"
	"github.com/dchest/blake2b"
	"github.com/gorilla/mux"
	"github.com/tsileo/blobstash/client/interface"
	"github.com/tsileo/blobstash/config/pathutil"
	"github.com/tsileo/blobstash/ext/docstore/id"
	log "gopkg.in/inconshreveable/log15.v2"
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
	nReturned           int
	totalDocsExamined   int
	executionTimeMillis int
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

func (docstore *DocStoreExt) RegisterRoute(r *mux.Router) {
	r.HandleFunc("/", docstore.CollectionsHandler())
	r.HandleFunc("/{collection}", docstore.DocsHandler())
	r.HandleFunc("/{collection}/search", docstore.SearchHandler())
	r.HandleFunc("/{collection}/{_id}", docstore.DocHandler())
}

func (docstore *DocStoreExt) SearchHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		collection := vars["collection"]
		if collection == "" {
			panic("missing collection query arg")
		}
		query := bleve.NewQueryStringQuery(r.URL.Query().Get("q"))
		searchRequest := bleve.NewSearchRequest(query)
		searchResult, err := docstore.index.Search(searchRequest)
		if err != nil {
			panic(err)
		}
		var docs []map[string]interface{}
		for _, sr := range searchResult.Hits {
			doc, err := docstore.fetchDoc(collection, sr.ID)
			if err != nil {
				panic(err)
			}
			docs = append(docs, doc)
		}
		WriteJSON(w, map[string]interface{}{
			"_meta": searchResult,
			"data":  docs,
		})
	}
}

// NextKey returns the next key for lexigraphical (key = NextKey(lastkey))
func NextKey(key string) string {
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

func (docstore *DocStoreExt) CollectionsHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "GET":
			collections := []string{}
			lastKey := ""
			for {
				ksearch := fmt.Sprintf("docstore:%v", lastKey)
				res, err := docstore.kvStore.Keys(ksearch, "\xff", 1)
				// docstore.logger.Debug("loop", "ksearch", ksearch, "len_res", len(res))
				if err != nil {
					panic(err)
				}
				if len(res) == 0 {
					break
				}
				col := strings.Split(res[0].Key, ":")[1]
				lastKey = NextKey(col)
				collections = append(collections, col)
			}
			WriteJSON(w, map[string]interface{}{
				"collections": collections,
			})
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}
}

func (docstore *DocStoreExt) DocsHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		collection := vars["collection"]
		if collection == "" {
			panic("missing collection query arg")
		}
		// FIXME(ts) returns the execution stats and add a debug mode in the CLI
		switch r.Method {
		case "GET":
			q := r.URL.Query()
			start := fmt.Sprintf(KeyFmt, collection, "") // q.Get("start"))
			// TODO(ts) check the \xff
			end := fmt.Sprintf(KeyFmt, collection, "\xff") // q.Get("end")+"\xff")
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
			res, err := docstore.kvStore.Keys(start, end, limit)
			if err != nil {
				panic(err)
			}
			var docs []map[string]interface{}
			for _, kv := range res {
				doc, err := docstore.fetchDoc(collection, hashFromKey(collection, kv.Key))
				if err != nil {
					panic(err)
				}
				if len(query) == 0 {
					// No query, so we just add every docs
					docs = append(docs, doc)
				} else {
					ok := true
					for key, eval := range query {
						if val, check := doc[key]; check {
							ok = ok && reflect.DeepEqual(eval, val)
						}
					}
					if ok {
						docs = append(docs, doc)
					}
				}
			}
			WriteJSON(w, map[string]interface{}{"data": docs,
				"_meta": map[string]interface{}{
					"limit": limit,
				},
			})
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
			docFlag := FlagNoIndex
			// Should the doc be full-text indexed?
			indexHeader := r.Header.Get("BlobStash-DocStore-IndexFullText")
			if indexHeader != "" {
				docFlag = FlagIndexed
			}
			// Store the payload in a blob
			hash := fmt.Sprintf("%x", blake2b.Sum256(blob))
			docstore.blobStore.Put(hash, blob)
			// Create a pointer in the key-value store
			now := time.Now().UTC().Unix()
			_id, err := id.New(int(now), hash)
			if err != nil {
				panic(err)
			}
			if _, err := docstore.kvStore.Put(fmt.Sprintf(KeyFmt, collection, _id.String()), string([]byte{docFlag}), -1); err != nil {
				panic(err)
			}
			// Returns the doc along with its new ID
			doc["_id"] = _id
			doc["_hash"] = hash
			doc["_created_at"] = _id.Ts()
			if indexHeader != "" {
				if err := docstore.index.Index(_id.String(), doc); err != nil {
					panic(err)
				}
			}
			WriteJSON(w, doc)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}
}

func (docstore *DocStoreExt) fetchDoc(collection, sid string) (map[string]interface{}, error) {
	if collection == "" {
		return nil, errors.New("missing collection query arg")
	}
	_id, err := id.FromHex(sid)
	if err != nil {
		return nil, fmt.Errorf("invalid _id: %v", err)
	}
	hash, err := _id.Hash()
	if err != nil {
		return nil, errors.New("failed to extract hash")
	}
	// Fetch the blob
	blob, err := docstore.blobStore.Get(hash)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch blob %v", hash)
	}
	// Build the doc
	doc := map[string]interface{}{}
	if err := json.Unmarshal(blob, &doc); err != nil {
		return nil, fmt.Errorf("failed to unmarshal blob: %s", blob)
	}
	doc["_id"] = _id
	doc["_hash"] = hash
	doc["_created_at"] = _id.Ts()
	return doc, nil
}

func (docstore *DocStoreExt) DocHandler() func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "GET":
			vars := mux.Vars(r)
			collection := vars["collection"]
			if collection == "" {
				panic("missing collection query arg")
			}
			sid := vars["_id"]
			if sid == "" {
				panic("missing _id query arg")
			}
			doc, err := docstore.fetchDoc(collection, sid)
			if err != nil {
				panic(err)
			}
			WriteJSON(w, doc)
		}
	}
}
