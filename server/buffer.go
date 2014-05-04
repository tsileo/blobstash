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

type ReqBuffer struct {
	dbmanager *DBsManager
	blobBackend backend.BlobHandler
	db *db.DB
	sync.Mutex
	reqCnt int
	Reqs map[string][]*ReqArgs
	ReqsKeyRef map[string]map[string]*ReqArgs
}

type ReqArgs struct {
	Key string `json:"key"`
	Args [][]string `json:"args"`
}

func NewReqBuffer(blobBackend backend.BlobHandler, dbmanager *DBsManager) *ReqBuffer {
	return &ReqBuffer{Reqs: make(map[string][]*ReqArgs),
					ReqsKeyRef: make(map[string]map[string]*ReqArgs),
					blobBackend: blobBackend, dbmanager: dbmanager}
}

func (rb *ReqBuffer) SetDB(db *db.DB) {
	rb.db = db
}

func (rb *ReqBuffer) reset() {
	rb.reqCnt = 0
	rb.Reqs = make(map[string][]*ReqArgs)
	rb.ReqsKeyRef = make(map[string]map[string]*ReqArgs)
}

func (rb *ReqBuffer) Add(reqType, reqKey string, reqArgs []string) (err error) {
	if strings.HasPrefix(reqKey, "_") {
		return
	}
	rb.Lock()
	defer rb.Unlock()
	if rb.reqCnt >= 1000 {
		err = rb.Save()
		if err != nil {
			return
		}
	}
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

func (rb *ReqBuffer) Save() error {
	if rb.reqCnt == 0 {
		return nil
	}
	h, d := rb.JSON()
	rb.reset()
	log.Printf("datadb: Meta blob:%v (len:%v) written\n", h, len(d))
	rb.db.Sadd("_meta", h)
	return rb.blobBackend.Put(h, d)
}

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
			for reqCmd, reqArgs := range res {
				switch {
				case reqCmd == "sadd":
					for _, req := range reqArgs {
						for _, args := range req.Args {
							rb.db.Sadd(req.Key, args...)
							log.Printf("datadb: Applying SADD: %+v/%+v", req.Key, args)
						}
					}

				case reqCmd == "hset":
					for _, req := range reqArgs {
						for _, args := range req.Args {
							rb.db.Hset(req.Key, args[0], args[1])
							log.Printf("datadb: Applying HSET: %+v/%+v", req.Key, args)
						}
					}

				case reqCmd == "hmset":
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
			rb.db.Sadd("_meta", hash)
		}
	}
	if err := <-errs; err != nil {
		return err
	}
	return nil
}

func (rb *ReqBuffer) JSON() (string, []byte) {
	data, _ := json.Marshal(rb.Reqs)
	sha1 := SHA1(data)
    return sha1, data
}

func (rb *ReqBuffer) Dump() {
	j, _ := rb.JSON()
	log.Printf("%v", string(j))
}

func (rb *ReqBuffer) Len() int {
	return rb.reqCnt
}

// TODO(tsileo) restore and apply
