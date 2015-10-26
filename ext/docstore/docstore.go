/*

Package docstore implements a JSON-based document store
built on top of the Versioned Key-Value store and the Blob store.

Each document will get assigned a MongoDB like ObjectId:

	<binary encoded uint32 (4 bytes) + blob ref (32 bytes)>

The resulting id will have a length of 72 characters encoded as hex.

The JSON document will be stored as is and kvk entry will reference it.

	docstore:<collection>:<id> => (empty)

The pointer contains an empty value since the hash is contained in the id.

Document will be automatically sorted by creation time thanks to the ID.

The raw JSON will be store unmodified but the API will add these fields on the fly:

 - `_id`: the hex ID
 - `_hash`: the hash of the JSON blob
 - `_created_at`: UNIX timestamp of creation date

*/
package docstore

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"github.com/dchest/blake2b"
	"github.com/tsileo/blobstash/ext/docstore/id"
	"github.com/tsileo/blobstash/router"
	"github.com/tsileo/blobstash/vkv"
)

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

func DocsHandler(blobrouter *router.Router, db *vkv.DB) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		// FIXME(ts) set the collection in path
		collection := r.URL.Query().Get("collection")
		if collection == "" {
			panic("missing collection query arg")
		}
		q := r.URL.Query()
		start := fmt.Sprint("docstore:%v:%v", collection, q.Get("start"))
		// TODO(ts) check the \xff
		end := fmt.Sprint("docstore:%v:%v", collection, q.Get("end")+"\xff")
		limit := 0
		if q.Get("limit") != "" {
			ilimit, err := strconv.Atoi(q.Get("limit"))
			if err != nil {
				http.Error(w, "bad limit", 500)
			}
			limit = ilimit
		}
		res, err := db.Keys(q.Get("start"), end, limit)
		if err != nil {
			panic(err)
		}
		var docs []map[string]interface{}
		for _, kv := range res {
			_id, err := id.FromHex(kv.Key)
			if err != nil {
				panic(err)
			}
			hash, err := _id.Hash()
			if err != nil {
				panic("failed to extract hash")
			}
			// Fetch the blob
			req := &router.Request{
				Type: router.Read,
				//	Namespace: r.URL.Query().Get("ns"),
			}
			backend := blobrouter.Route(req)
			blob, err := backend.Get(hash)
			if err != nil {
				panic(err)
			}
			// Build the doc
			doc := map[string]interface{}{}
			if err := json.Marshal(blob, &doc); err != nil {
				panic(err)
			}
			doc["_id"] = _id
			doc["_hash"] = hash
			doc["_created_at"] = _id.TS()
			docs = append(docs, doc)
		}
		WriteJSON(w, map[string]interface{}{"data": res, "start": start, "end": end, "limit": limit})
	}
}

func NewDocHandler(blobs chan<- *router.Blob, blobrouter *router.Router, db *vkv.DB, kvUpdate chan *vkv.KeyValue) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "GET":
			// FIXME(ts) set the collection in path
			collection := r.URL.Query().Get("collection")
			if collection == "" {
				panic("missing collection query arg")
			}
			sid := r.URL.Query().Get("_id")
			if sid != "" {
				panic("missing _id query arg")
			}
			// Parse the hex ID
			_id, err := id.FromHex(sid)
			if err != nil {
				panic(fmt.Sprintf("invalid _id: %v", err))
			}
			hash, err := _id.Hash()
			if err != nil {
				panic("failed to extract hash")
			}
			// Fetch the blob
			req := &router.Request{
				Type: router.Read,
				//	Namespace: r.URL.Query().Get("ns"),
			}
			backend := blobrouter.Route(req)
			blob, err := backend.Get(hash)
			if err != nil {
				panic(err)
			}
			// Build the doc
			doc := map[string]interface{}{}
			if err := json.Marshal(blob, &doc); err != nil {
				panic(err)
			}
			doc["_id"] = _id
			doc["_hash"] = hash
			doc["_created_at"] = _id.TS()
			WriteJSON(w, doc)
		case "POST":
			// FIXME(ts) set the collection in path
			collection := r.URL.Query().Get("collection")
			if collection == "" {
				panic("missing collection query arg")
			}
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
			// Store the payload in a blob
			hash := fmt.Sprintf("%x", blake2b.Sum256(blob))
			req := &router.Request{
				Type: router.Write,
				//	Namespace: r.URL.Query().Get("ns"),
			}
			blobs <- &router.Blob{Hash: hash, Req: req, Blob: blob}
			// Create a pointer in the key-value store
			now := time.Now().UTC().Unix()
			_id, err := id.New(int(now), hash)
			if err != nil {
				panic(err)
			}
			res, err := db.Put(fmt.Sprintf("docstore:%v:%v", collection, _id.String()), "", -1)
			if err != nil {
				panic(err)
			}
			kvUpdate <- res
			// Returns the doc along with its new ID
			doc["_id"] = _id
			doc["_hash"] = hash
			doc["_created_at"] = id.TS()
			WriteJSON(w, doc)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}
}
