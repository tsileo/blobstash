package blob // import "a4.io/blobstash/pkg/blob"

import (
	"bytes"
	"fmt"

	"a4.io/blobstash/pkg/hashutil"
)

var (
	nodeHeader = []byte("#blob/node\n")
	metaHeader = []byte("#blob/meta\n")
	// FIXME(tsileo): #blob/doc\n header
)

// FIXME(tsileo): remove this
type NamespacedBlobRef struct {
	Hash      string
	Size      int
	Namespace string
}

type SizedBlobRef struct {
	Hash string `json:"hash"`
	Size int    `json:"size"`
}

type Blob struct {
	Hash  string      `json:"h"`
	Data  []byte      `json":"-"`
	Extra interface{} `json:"e,omitempty"`
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

func (b *Blob) IsMeta() bool {
	if len(b.Data) < len(metaHeader) {
		return false
	}
	if bytes.Equal(b.Data[0:len(metaHeader)], metaHeader) {
		return true
	}
	return false
}

func (b *Blob) IsFiletreeNode() bool {
	if len(b.Data) < len(nodeHeader) {
		return false
	}
	if bytes.Equal(b.Data[0:len(nodeHeader)], nodeHeader) {
		return true
	}
	return false
}
