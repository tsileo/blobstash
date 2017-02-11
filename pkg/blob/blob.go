package blob // import "a4.io/blobstash/pkg/blob"

import (
	"fmt"

	"a4.io/blobstash/pkg/hashutil"
)

// FIXME(tsileo): remove this
type NamespacedBlobRef struct {
	Hash      string
	Size      int
	Namespace string
}

type SizedBlobRef struct {
	Hash string
	Size int
}

type Blob struct {
	Hash string `json:"h"`
	Data []byte `json":"-"`
}

// String implements the Stringer interface
func (b *Blob) String() string {
	return b.Hash
}

func New(data []byte) *Blob {
	return &Blob{
		Data: data,
		Hash: hashutil.Compute(data),
	}
}

func (b *Blob) Check() error {
	chash := hashutil.Compute(b.Data)
	if b.Hash != chash {
		return fmt.Errorf("Hash mismatch: given=%s, computed=%v", b.Hash, chash)
	}
	return nil
}
