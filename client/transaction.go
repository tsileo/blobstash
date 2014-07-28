package client

import (
	"github.com/tsileo/blobstash/client/ctx"
	"github.com/tsileo/blobstash/client/transaction"
)

func NewTransaction() *transaction.Transaction {
	return transaction.NewTransaction()
}

func (client *Client) Commit(cctx *ctx.Ctx, tx *transaction.Transaction) error {
	hash, js := tx.Dump()
	if err := client.BlobStore.Put(cctx.Meta(), hash, js); err != nil {
		return err
	}
	return nil
}
