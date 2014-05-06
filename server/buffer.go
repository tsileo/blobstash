/*

Each request that modify the DB is added to the ReqBuffer, the buffered commands are:

- SET
- HMSET/HSET
- LADD
- SADD

If an upload fails client-side, no operations will be committed, and no stale date will be saved.

TODO(tsileo) More docs on transaction handling.

Blobs operations aren't buffered since it's handled by a different BlobHandler.

When a backup/snapshot is done, the client request a dump and the blob is added to the Meta BlobHandler.
The blob is also flagged (added in the **_meta** set) as applied so it won't be re-applied at startup.

If the buffer size grows up to 1000, it will be saved to disk immediately, and reseted.

This process allows to be able to re-index in case of a DB loss (and it will also be useful when replicating).

At startup, the blobs hash will be check and applied if missing.

Example of what a Meta blob looks like:

	{
	  "hmset": [
	    {
	      "args": [
	        [
	          "name", 
	          "writing", 
	          "type", 
	          "dir", 
	          "ref", 
	          "666d51cc63367a434d8ded9f336b3ac9f7188547", 
	          "ts", 
	          "1399219126"
	        ]
	      ], 
	      "key": "backup:e23c16bcc4e5ffcfaf81ec9627a7753cb2b55d0a"
	    }
	  ], 
	  "ladd": [
	    {
	      "args": [
	        [
	          "1399219126", 
	          "backup:e23c16bcc4e5ffcfaf81ec9627a7753cb2b55d0a"
	        ]
	      ], 
	      "key": "writing"
	    }
	  ], 
	  "sadd": [
	    {
	      "args": [
	        [
	          "writing"
	        ]
	      ], 
	      "key": "filenames"
	    }
	  ]
	}

*/
package server

import (
	"github.com/tsileo/datadatabase/backend"
	"github.com/tsileo/datadatabase/db"
	"sync"
	"strings"
	"log"
	"strconv"
	"encoding/json"
)

type TxManager struct {
	Txs map[string]*ReqBuffer
	db *db.DB
	blobBackend backend.BlobHandler
	sync.Mutex
}

type ReqBuffer struct {
	blobBackend backend.BlobHandler
	db *db.DB
	*sync.Mutex
	reqCnt int
	Reqs map[string][]*ReqArgs
	ReqsKeyRef map[string]map[string]*ReqArgs
}

// ReqArgs store the list of args (list of string) for the given key.
type ReqArgs struct {
	Key string `json:"key"`
	Args [][]string `json:"args"`
}

// NewTxManager initialize a new TxManager for the given db.
func NewTxManager(cdb *db.DB, blobBackend backend.BlobHandler) *TxManager {
	return &TxManager{make(map[string]*ReqBuffer), cdb, blobBackend, sync.Mutex{}}
}

// GetReqBuffer retrieves an existing ReqBuffer or create it if it doesn't exists yet.
func (txm *TxManager) GetReqBuffer(name string) *ReqBuffer {
	txm.Lock()
	defer txm.Unlock()
	rb, rbExists := txm.Txs[name]
	if !rbExists {
		txm.Txs[name] = NewReqBuffer(txm.db, txm.blobBackend)
		return txm.Txs[name]
	}
	return rb
}

// NewReqBuffer initialize a new ReqBuffer
func NewReqBuffer(cdb *db.DB, blobBackend backend.BlobHandler) *ReqBuffer {
	return &ReqBuffer{blobBackend, cdb, &sync.Mutex{}, 0, make(map[string][]*ReqArgs),
					make(map[string]map[string]*ReqArgs)}
}

// NewReqBufferWithData is a wrapper around NewReqBuffer, it fills the buffer with the given data.
func NewReqBufferWithData(cdb *db.DB, blobBackend backend.BlobHandler, data map[string][]*ReqArgs) *ReqBuffer {
	rb := NewReqBuffer(cdb, blobBackend)
	rb.Reqs = data
	return rb
}

// Reset the buffer.
func (rb *ReqBuffer) Reset() {
	rb.Lock()
	defer rb.Unlock()
	rb.reqCnt = 0
	rb.Reqs = make(map[string][]*ReqArgs)
	rb.ReqsKeyRef = make(map[string]map[string]*ReqArgs)
}

// Add a server request to the Buffer, requests are factorized to reduce blob size.
func (rb *ReqBuffer) Add(reqType, reqKey string, reqArgs []string) (err error) {
	if strings.HasPrefix(reqKey, "_") {
		return
	}
	rb.Lock()
	defer rb.Unlock()
	rb.reqCnt++
	_, exists := rb.Reqs[reqType]
	if !exists {
		rb.Reqs[reqType] = []*ReqArgs{}
		rb.ReqsKeyRef[reqType] = make(map[string]*ReqArgs)
	}
	_, exists = rb.ReqsKeyRef[reqType][reqKey]
	if !exists {
		ra := &ReqArgs{Key: reqKey, Args:[][]string{reqArgs}}
		rb.Reqs[reqType] = append(rb.Reqs[reqType], ra)
		rb.ReqsKeyRef[reqType][reqKey] = ra
	} else {
		rb.ReqsKeyRef[reqType][reqKey].Args = append(rb.ReqsKeyRef[reqType][reqKey].Args, reqArgs)
	}
	return
}

// Put the blob to Meta BlobHandler.
func (rb *ReqBuffer) Save() error {
	if rb.reqCnt == 0 {
		return nil
	}
	h, d := rb.JSON()
	log.Printf("datadb: Meta blob:%v (%v commands, len:%v) written\n", h, rb.reqCnt, len(d))
	rb.db.Sadd("_meta", h)
	return rb.blobBackend.Put(h, d)
}

// Enumerate every meta blobs filename and check if the data is already indexed.
func (rb *ReqBuffer) Load() error {
	hashes := make(chan string)
	errs := make(chan error)
	go func() {
		errs <- rb.blobBackend.Enumerate(hashes)
	
	}()
	for hash := range hashes {
		cnt := rb.db.Sismember("_meta", hash)
		if cnt == 0 {
			log.Printf("datadb: Found a Meta blob not loaded %v\n", hash)
			data, berr := rb.blobBackend.Get(hash)
			if berr != nil {
				return berr
			}
			// TODO(tsileo) check error
			res := make(map[string][]*ReqArgs)
			json.Unmarshal(data, &res)
			err := NewReqBufferWithData(rb.db, rb.blobBackend, res).Apply()
			if err != nil {
				return err
			}
		}
	}
	if err := <-errs; err != nil {
		return err
	}
	return nil
}

// Dump the buffer as JSON.
func (rb *ReqBuffer) JSON() (string, []byte) {
	data, _ := json.Marshal(rb.Reqs)
	sha1 := SHA1(data)
    return sha1, data
}

// Return the number of commands stored.
func (rb *ReqBuffer) Len() int {
	return rb.reqCnt
}

// ApplyReqArgs actually execute each commands stored in a ReqArgs.
func (rb *ReqBuffer) Apply() error {
	for reqCmd, reqArgs := range rb.Reqs {
		switch {
		case reqCmd == "sadd":
			for _, req := range reqArgs {
				for _, args := range req.Args {
					rb.db.Sadd(req.Key, args...)
					log.Printf("datadb: Applying SADD: %+v/%+v", req.Key, args)
				}
			}

		case reqCmd == "hmset" || reqCmd == "hset":
			for _, req := range reqArgs {
				for _, args := range req.Args {
					rb.db.Hmset(req.Key, args...)
					log.Printf("datadb: Applying HMSET: %+v/%+v", req.Key, args)
				}
			}

		case reqCmd == "ladd":
			for _, req := range reqArgs {
				for _, args := range req.Args {
					index, ierr := strconv.Atoi(args[0])
					if ierr != nil {
						log.Printf("datadb: Bad LADD index: %v, err:%v", index, ierr)
						return ierr
					}
					rb.db.Ladd(req.Key, index, args[1])
					log.Printf("datadb: Applying LADD: %+v/%+v", req.Key, args)
				}
			}

		case reqCmd == "set":
			for _, req := range reqArgs {
				for _, args := range req.Args {
					rb.db.Put(req.Key, args[0])
					log.Printf("datadb: Applying SET: %+v/%+v", req.Key, args)
				}
			}

		}
	}
	hash, _ := rb.JSON()
	rb.db.Sadd("_meta", hash)
	return nil
}

// TODO(tsileo) restore and apply
