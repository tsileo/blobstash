package client2

import (
	"strconv"
	"github.com/tsileo/blobstash/reqbuffer"
	"github.com/tsileo/blobstash/client2/ctx"
)

type Transaction struct {
	ReqBuffer *reqbuffer.ReqBuffer
}

func (client *Client) NewTransaction() *Transaction {
	return &Transaction{
		ReqBuffer: reqbuffer.NewReqBuffer(),
	}
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

func (tx *Transaction) dump() (string, []byte) {
	tx.ReqBuffer.Lock()
	defer tx.ReqBuffer.Unlock()
	return tx.ReqBuffer.JSON()
}

func (client *Client) Commit(cctx *ctx.Ctx, tx *Transaction) error {
	hash, js := tx.dump()
	if err := client.BlobStore.Put(&ctx.Ctx{MetaBlob: true, Namespace: cctx.Namespace}, hash, js); err != nil {
		return err
	}
	return nil
}