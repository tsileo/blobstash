/*

Package node implement the node specification for the filetree extension.

*/
package node // import "a4.io/blobstash/pkg/filetree/filetreeutil/node"

import (
	"bytes"
	"fmt"
	"mime"
	"path/filepath"

	"github.com/vmihailenco/msgpack"
	"golang.org/x/crypto/blake2b"
)

var (
	NodeBlobHeader          = []byte("#blob/node\n")
	NodeBlobMsgpackEncoding = byte('1')
	NodeBlobOverhead        = len(NodeBlobHeader) + 1
)

const (
	File = "file"
	Dir  = "dir"
)

const (
	V1 = "1"
)

func IsNodeBlob(blob []byte) ([]byte, bool) { // returns (string, bool) string => meta type
	// TODO add a test with a tiny blob
	if len(blob) < NodeBlobOverhead {
		return nil, false
	}
	if bytes.Equal(blob[0:NodeBlobOverhead-1], []byte(NodeBlobHeader)) {
		return blob[NodeBlobOverhead:len(blob)], true
	}
	return nil, false
}

type IndexValue struct {
	Index int64  `json:"i" msgpack:"i"`
	Value string `json:"ref" msgpack:"r"`
}

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

func (n *RawNode) FileRefs() []*IndexValue {
	var out []*IndexValue
	if n.Size > 0 {
		for _, m := range n.Refs {
			data := m.([]interface{})
			var index int64
			switch i := data[0].(type) {
			case float64:
				index = int64(i)
			case int:
				index = int64(i)
			case int64:
				index = i
			case int8:
				index = int64(i)
			case int16:
				index = int64(i)
			case int32:
				index = int64(i)

			// XXX(tsileo): these a used by msgpack
			case uint8:
				index = int64(i)
			case uint16:
				index = int64(i)
			case uint32:
				index = int64(i)
			case uint64:
				index = int64(i)
			default:
				panic("unexpected index")
			}
			iv := &IndexValue{Index: index, Value: data[1].(string)}
			out = append(out, iv)
		}
	}
	return out
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
	data := append(NodeBlobHeader, NodeBlobMsgpackEncoding)
	data = append(data, js...)
	h := fmt.Sprintf("%x", blake2b.Sum256(data))
	return h, data
}

func (n *RawNode) AddIndexedRef(index int, hash string) {
	n.Refs = append(n.Refs, []interface{}{index, hash})
}

func (n *RawNode) AddRef(hash string) {
	n.Refs = append(n.Refs, hash)
}

func NewNodeFromBlob(hash string, blob []byte) (*RawNode, error) {
	node := &RawNode{}
	data, ok := IsNodeBlob(blob)
	if ok {
		blob = data
	}
	//if !ok {
	//	return nil, fmt.Errorf("not a node blob")
	//}
	if err := msgpack.Unmarshal(blob, node); err != nil {
		return nil, err
	}
	if !ok {
		fmt.Printf("\n\n\nBLOB WITHOUT NODE HEADER=%s/%+v\n\n\n", hash, node)
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
