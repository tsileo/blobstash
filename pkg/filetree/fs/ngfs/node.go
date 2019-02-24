package main

import (
	"time"

	rnode "a4.io/blobstash/pkg/filetree/filetreeutil/node"
)

// node represents a FileTree node
type node struct {
	Name       string                 `json:"name" msgpack:"n"`
	Ref        string                 `json:"ref" msgpack:"r"`
	Size       int                    `json:"size" msgpack:"s,omitempty"`
	Type       string                 `json:"type" msgpack:"t"`
	Children   []*node                `json:"children" msgpack:"c,omitempty"`
	Metadata   map[string]interface{} `json:"metadata" msgpack:"md,omitempty"`
	ModTime    string                 `json:"mtime" msgpack:"mt"`
	ChangeTime string                 `json:"ctime" msgpack:"ct"`
	RawMode    int                    `json:"mode" msgpack:"mo"`
	RemoteRefs []*rnode.IndexValue    `json:"remote_refs,omitempty" msgpack:"rrfs,omitempty"`
	Info       map[string]interface{} `json:"info,omitempty" msgpack:"i,omitempty"`

	// Set by the FS
	AsOf int64 `json:"-" msgpack:"-"`
}

// mode returns the node file mode
func (n *node) mode() uint32 {
	// TODO(tsileo): handle asOf
	if n.RawMode > 0 {
		return uint32(n.RawMode)
	}
	if n.Type == rnode.File {
		return 0644
	} else {
		return 0755
	}
}

// hash returns the file content hash (blake2b)
func (n *node) hash() string {
	if len(n.Metadata) == 0 {
		// It happens for empty file
		return "69217a3079908094e11121d042354a7c1f55b6482ca1a51e1b250dfd1ed0eef9"
	}
	return n.Metadata["blake2b-hash"].(string)
}

// isDir returns true if the node is a dir
func (n *node) isDir() bool {
	return n.Type == rnode.Dir
}

// is File returns true if the node is a file
func (n *node) isFile() bool {
	return n.Type == rnode.File
}

// mtime returns the node mtime timestsamp
func (n *node) mtime() uint64 {
	if n.ModTime != "" {
		t, err := time.Parse(time.RFC3339, n.ModTime)
		if err != nil {
			panic(err)
		}
		return uint64(t.Unix())
	}
	return 0
}

// ctime returns the node ctime timestamp
func (n *node) ctime() uint64 {
	if n.ChangeTime != "" {
		t, err := time.Parse(time.RFC3339, n.ChangeTime)
		if err != nil {
			panic(err)
		}
		return uint64(t.Unix())
	}
	return 0
}
