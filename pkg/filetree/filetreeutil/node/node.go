/*

Package node implement the node specification for the filetree extension.

*/
package node // import "a4.io/blobstash/pkg/filetree/filetreeutil/node"

import (
	"fmt"
	"mime"
	"path/filepath"

	"github.com/dchest/blake2b"
	"github.com/vmihailenco/msgpack"
)

var (
	version = "1"
)

type RawNode struct {
	ModTime    int64                  `msgpack:"mt,omitempty"`
	ChangeTime int64                  `msgpack:"ct,omitempty"`
	Mode       uint32                 `msgpack:"mo,omitempty"`
	Name       string                 `msgpack:"n"`
	Type       string                 `msgpack:"t"`
	Size       int                    `msgpack:"s"`
	Refs       []interface{}          `msgpack:"r"`
	Version    string                 `msgpack:"v"`
	Metadata   map[string]interface{} `msgpack:"m,omitempty"`
	Hash       string                 `msgpack:"-"`
}

// AddData insert a new meta data in the Data field
func (n *RawNode) AddData(key string, val interface{}) {
	if n.Metadata == nil {
		n.Metadata = map[string]interface{}{}
	}
	n.Metadata[key] = val
}

// JSON returns the `Node` encoded as (hash, blob)
func (n *RawNode) Encode() (string, []byte) {
	js, err := msgpack.Marshal(n)
	if err != nil {
		panic(err)
	}
	h := fmt.Sprintf("%x", blake2b.Sum256(js))
	return h, js
}

func (n *RawNode) AddIndexedRef(index int, hash string) {
	n.Refs = append(n.Refs, []interface{}{index, hash})
}

func (n *RawNode) AddRef(hash string) {
	n.Refs = append(n.Refs, hash)
}

func NewNodeFromBlob(hash string, blob []byte) (*RawNode, error) {
	node := &RawNode{}
	if err := msgpack.Unmarshal(blob, node); err != nil {
		return nil, err
	}
	node.Hash = hash
	return node, nil
}

func (n *RawNode) ContentType() string {
	if n.IsFile() {
		return mime.TypeByExtension(filepath.Ext(n.Name))
	}
	return ""
}

// IsFile returns true if the Meta is a file.
func (n *RawNode) IsFile() bool {
	if n.Type == "file" {
		return true
	}
	return false
}
