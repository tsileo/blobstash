package transaction

import (
	"github.com/tsileo/blobstash/reqbuffer"
	"strconv"
)

type Transaction struct {
	ReqBuffer *reqbuffer.ReqBuffer
}

func NewTransaction() *Transaction {
	return &Transaction{
		ReqBuffer: reqbuffer.NewReqBuffer(),
	}
}

func (tx *Transaction) Merge(tx2 *Transaction) error {
	return tx.ReqBuffer.Merge(tx2.ReqBuffer)
}

func (tx *Transaction) Len() int {
	return tx.ReqBuffer.Len()
}

func (tx *Transaction) Reset() {
	tx.ReqBuffer.Reset()
}

func (tx *Transaction) Sadd(key string, members ...string) {
	tx.ReqBuffer.Add("sadd", key, members)
}

func (tx *Transaction) Hmset(key string, fieldValues ...string) {
	tx.ReqBuffer.Add("hmset", key, fieldValues)
}

func (tx *Transaction) Ladd(key string, index int, value string) {
	tx.ReqBuffer.Add("ladd", key, []string{strconv.Itoa(index), value})
}

func (tx *Transaction) Set(key, value string) {
	tx.ReqBuffer.Add("set", key, []string{value})
}

func (tx *Transaction) Dump() (string, []byte) {
	tx.ReqBuffer.Lock()
	defer tx.ReqBuffer.Unlock()
	return tx.ReqBuffer.JSON()
}
