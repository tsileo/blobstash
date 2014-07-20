/*

Package reqbuffer implements a request buffer, it powers BlobDB transctions.

*/
package reqbuffer

import (
	"encoding/json"
	"fmt"
	"bytes"
	"sync"
	"crypto/sha1"
)

var (
	MetaBlobHeader = "#blobstash/meta\n"
	MetaBlobOverhead = len(MetaBlobHeader)
)

type ReqBuffer struct {
	reqCnt     int
	Reqs       map[string][]*ReqArgs
	ReqsKeyRef map[string]map[string]*ReqArgs
	sync.Mutex
}

// ReqArgs store the list of args (list of string) for the given key.
type ReqArgs struct {
	Key  string     `json:"key"`
	Args [][]string `json:"args"`
}

// NewReqBuffer initialize a new ReqBuffer
func NewReqBuffer() *ReqBuffer {
	return &ReqBuffer{
		Reqs: make(map[string][]*ReqArgs),
		ReqsKeyRef: make(map[string]map[string]*ReqArgs),
	}
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

// Dump the buffer as JSON, it returns the hash and the JSON.
func (rb *ReqBuffer) JSON() (string, []byte) {
	// Build the blob
	var blob bytes.Buffer
	blob.WriteString(MetaBlobHeader)
	data, _ := json.Marshal(rb.Reqs)
	blob.Write(data)
	// Compute the blob hash
	h := sha1.New()
	h.Write(blob.Bytes())
	sha1 := fmt.Sprintf("%x", h.Sum(nil))
	return sha1, blob.Bytes()
}

// Return the number of commands stored.
func (rb *ReqBuffer) Len() int {
	return rb.reqCnt
}

// Merge merge the given ReqBuffer.
func (rb *ReqBuffer) Merge(rb2 *ReqBuffer) error {
	rb2.Lock()
	defer rb2.Unlock()
	for reqType, reqArgs := range rb2.Reqs {
		for _, req := range reqArgs {
			for _, args := range req.Args {
				if err := rb.Add(reqType, req.Key, args); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
