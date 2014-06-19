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
	"encoding/json"
	"fmt"
	"bytes"
	"strconv"
	"strings"
	"sync"

	"github.com/tsileo/blobstash/backend"
	"github.com/tsileo/blobstash/db"
)

var (
	MetaBlobHeader = "#blobstash/meta\n"
	MetaBlobOverhead = len(MetaBlobHeader)
)

type TxManager struct {
	Txs         map[string]*ReqBuffer
	db          *db.DB
	blobBackend *backend.Router
	sync.Mutex
}

type ReqBuffer struct {
	blobBackend *backend.Router
	db          *db.DB
	*sync.Mutex
	reqCnt     int
	Reqs       map[string][]*ReqArgs
	ReqsKeyRef map[string]map[string]*ReqArgs
}

// ReqArgs store the list of args (list of string) for the given key.
type ReqArgs struct {
	Key  string     `json:"key"`
	Args [][]string `json:"args"`
}

// NewTxManager initialize a new TxManager for the given db.
func NewTxManager(cdb *db.DB, blobBackend *backend.Router) *TxManager {
	return &TxManager{make(map[string]*ReqBuffer), cdb, blobBackend, sync.Mutex{}}
}

// GetReqBuffer retrieves an existing ReqBuffer or create it if it doesn't exists yet.
func (txm *TxManager) GetReqBuffer(name string) *ReqBuffer {
	txm.Lock()
	defer txm.Unlock()
	rb, rbExists := txm.Txs[name]
	if !rbExists {
		txm.Txs[name] = NewReqBuffer(txm.db, txm.blobBackend)
		//go txm.Txs[name].Load()
		return txm.Txs[name]
	}
	return rb
}

// NewReqBuffer initialize a new ReqBuffer
func NewReqBuffer(cdb *db.DB, blobBackend *backend.Router) *ReqBuffer {
	return &ReqBuffer{blobBackend, cdb, &sync.Mutex{}, 0, make(map[string][]*ReqArgs),
		make(map[string]map[string]*ReqArgs)}
}

// NewReqBufferWithData is a wrapper around NewReqBuffer, it fills the buffer with the given data.
func NewReqBufferWithData(cdb *db.DB, blobBackend *backend.Router, data map[string][]*ReqArgs) *ReqBuffer {
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
		ra := &ReqArgs{Key: reqKey, Args: [][]string{reqArgs}}
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
	go SendDebugData(fmt.Sprintf("server: meta blob:%v (len:%v) written\n", h, len(d)))
	rb.Reset()
	if err := rb.blobBackend.MetaPut(h, d); err != nil {
		return fmt.Errorf("Error putting blob: %v", err)
	}
	if _, err := rb.db.Sadd("_meta", h); err != nil {
		return fmt.Errorf("Error adding the meta blob %v to _meta list: %v", h, err)
	}
	return nil
}

// Enumerate every meta blobs filename and check if the data is already indexed.
func (rb *ReqBuffer) Load() error {
	go SendDebugData("server: scanning meta blobs")
	//rb.Lock()
	//defer rb.Unlock()
	hashes := make(chan string)
	errc := make(chan error, 1)
	go func() {
		errc <- rb.blobBackend.MetaEnumerate(hashes)
	}()
	for hash := range hashes {
		cnt := rb.db.Sismember("_meta", hash)
		if cnt == 0 {
			blob, berr := rb.blobBackend.MetaGet(hash)
			if berr != nil {
				return berr
			}
			if !bytes.Equal(blob[0:MetaBlobOverhead], []byte(MetaBlobHeader)) {
				go SendDebugData(fmt.Sprintf("server: blob %v is not a valid meta blob, skipping", hash))
				continue
			}
			go SendDebugData(fmt.Sprintf("server: meta blob %v not yet loaded", hash))

			res := make(map[string][]*ReqArgs)

			if err := json.Unmarshal(blob[MetaBlobOverhead:], &res); err != nil {
				return err
			}

			if err := NewReqBufferWithData(rb.db, rb.blobBackend, res).Apply(); err != nil {
				return err
			}
			go SendDebugData(fmt.Sprintf("server: meta blob %v applied", hash))
		}
	}
	if err := <-errc; err != nil {
		go SendDebugData(fmt.Sprintf("server: aborting scan, err:%v", err))
		return err
	}
	go SendDebugData("server: scan done")
	return nil
}

// Dump the buffer as JSON.
func (rb *ReqBuffer) JSON() (string, []byte) {
	var blob bytes.Buffer
	blob.WriteString(MetaBlobHeader)
	data, _ := json.Marshal(rb.Reqs)
	blob.Write(data)
	sha1 := SHA1(blob.Bytes())
	return sha1, blob.Bytes()
}

// Return the number of commands stored.
func (rb *ReqBuffer) Len() int {
	return rb.reqCnt
}

// ApplyReqArgs execute each commands stored in a ReqArgs in a transaction.
func (rb *ReqBuffer) Apply() error {
	//commit := false
	//defer func() {
	//	if !commit {
	//		go SendDebugData(fmt.Sprintf("server: error applying ReqBuffer %+v, rolling back...", rb))
	//		rb.db.Rollback()
	//	}
	//}()
	//if err := rb.db.BeginTransaction(); err != nil {
	//	return err
	//}
	for reqCmd, reqArgs := range rb.Reqs {
		switch {
		case reqCmd == "sadd":
			for _, req := range reqArgs {
				for _, args := range req.Args {
					go SendDebugData(fmt.Sprintf("server: Applying SADD: %+v/%+v", req.Key, args))
					if _, err := rb.db.Sadd(req.Key, args...); err != nil {
						return err
					}
				}
			}

		case reqCmd == "hmset" || reqCmd == "hset":
			for _, req := range reqArgs {
				for _, args := range req.Args {
					go SendDebugData(fmt.Sprintf("server: Applying HMSET: %+v/%+v", req.Key, args))
					if _, err := rb.db.Hmset(req.Key, args...); err != nil {
						return err
					}
				}
			}

		case reqCmd == "ladd":
			for _, req := range reqArgs {
				for _, args := range req.Args {
					index, ierr := strconv.Atoi(args[0])
					if ierr != nil {
						go SendDebugData(fmt.Sprintf("server: Bad LADD index: %v, err:%v", index, ierr))
						return ierr
					}
					go SendDebugData(fmt.Sprintf("server: Applying LADD: %+v/%+v", req.Key, args))
					if err := rb.db.Ladd(req.Key, index, args[1]); err != nil {
						return err
					}
				}
			}

		case reqCmd == "set":
			for _, req := range reqArgs {
				for _, args := range req.Args {
					go SendDebugData(fmt.Sprintf("server: Applying SET: %+v/%+v", req.Key, args))
					if err := rb.db.Put(req.Key, args[0]); err != nil {
						return err
					}
				}
			}

		}
	}
	//commit = true
	//if err := rb.db.Commit(); err != nil {
	//	return err
	//}
	hash, _ := rb.JSON()
	rb.db.Sadd("_meta", hash)
	return nil
}

// TODO(tsileo) restore and apply
