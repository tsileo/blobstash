package server

import (
	"github.com/tsileo/datadatabase/backend"
	"github.com/tsileo/datadatabase/db"
	"sync"
	"strings"
	"log"
	"encoding/json"
)

type ReqBuffer struct {
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

func NewReqBuffer(blobBackend backend.BlobHandler) *ReqBuffer {
	return &ReqBuffer{Reqs: make(map[string][]*ReqArgs),
					ReqsKeyRef: make(map[string]map[string]*ReqArgs),
					blobBackend: blobBackend}
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
	if rb.reqCnt >= 5 {
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
	h, d := rb.JSON()
	rb.reset()
	log.Printf("datadb: Meta blob:%v (len:%v) written\n", h, len(d))
	return rb.blobBackend.Put(h, d)
}

func (rb *ReqBuffer) Load() {
	hashes := make(chan string)
	log.Println("before enumerate")
	errs := make(chan error)
	go func() {
		errs <- rb.blobBackend.Enumerate(hashes)
	
	}()
	log.Println("after enumerate")
	for hash := range hashes {
		log.Printf("datadb: Found meta blob:%v", hash)
	}
	if err := <-errs; err != nil {
		panic(err)
	}
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
