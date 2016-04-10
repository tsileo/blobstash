package blob

import (
	"fmt"

	"github.com/tsileo/blobstash/pkg/hashutil"
)

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
	Hash string
	Data []byte
}

func (b *Blob) Check() error {
	chash := hashutil.Compute(b.Data)
	if b.Hash != chash {
		return fmt.Errorf("Hash mismatch: given=%s, computed=%v", b.Hash, chash)
	}
	return nil
}
