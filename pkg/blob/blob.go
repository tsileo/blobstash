package blob // import "a4.io/blobstash/pkg/blob"

import (
	"bytes"
	"fmt"

	"a4.io/blobstash/pkg/hashutil"
)

var (
	nodeHeader = []byte("#blobstash/node\n")
	metaHeader = []byte("#blobstash/meta\n")
	// FIXME(tsileo): #blobstash/doc\n header
)

// SizedBlobRef holds a blob hash and its size
type SizedBlobRef struct {
	Hash string `json:"hash"`
	Size int    `json:"size"`
}

// Blob holds a blob hash/data pair
// Extra is here to temporarily attach related data
type Blob struct {
	Hash  string      `json:"h"`
	Data  []byte      `json:"-"`
	Extra interface{} `json:"e,omitempty"`
}

// String implements the Stringer interface
func (b *Blob) String() string {
	return b.Hash
}

// New initializes a new blob
func New(data []byte) *Blob {
	return &Blob{
		Data: data,
		Hash: hashutil.Compute(data),
	}
}

// Check ensures the hash match the data
func (b *Blob) Check() error {
	chash := hashutil.Compute(b.Data)
	if b.Hash != chash {
		return fmt.Errorf("Hash mismatch: given=%s, computed=%v", b.Hash, chash)
	}
	return nil
}

// IsMeta returns true if the blob contains "meta blob (an encoded internal data)
func (b *Blob) IsMeta() bool {
	if len(b.Data) < len(metaHeader) {
		return false
	}
	if bytes.Equal(b.Data[0:len(metaHeader)], metaHeader) {
		return true
	}
	return false
}

// IsFiletreeNode returns true if the blob contains a filetree node (an encoded file/dir meta data)
func (b *Blob) IsFiletreeNode() bool {
	if len(b.Data) < len(nodeHeader) {
		return false
	}
	if bytes.Equal(b.Data[0:len(nodeHeader)], nodeHeader) {
		return true
	}
	return false
}
