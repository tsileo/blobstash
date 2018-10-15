package main

import (
	"time"

	rnode "a4.io/blobstash/pkg/filetree/filetreeutil/node"
)

type Node struct {
	Name       string                 `json:"name" msgpack:"n"`
	Ref        string                 `json:"ref" msgpack:"r"`
	Size       int                    `json:"size" msgpack:"s,omitempty"`
	Type       string                 `json:"type" msgpack:"t"`
	Children   []*Node                `json:"children" msgpack:"c,omitempty"`
	Metadata   map[string]interface{} `json:"metadata" msgpack:"md,omitempty"`
	ModTime    string                 `json:"mtime" msgpack:"mt"`
	ChangeTime string                 `json:"ctime" msgpack:"ct"`
	RawMode    int                    `json:"mode" msgpack:"mo"`
	RemoteRefs []*rnode.IndexValue    `json:"remote_refs,omitempty" msgpack:"rrfs,omitempty"`

	// Set by the FS
	AsOf int64 `json:"-" msgpack:"-"`
}

func (n *Node) Mode() uint32 {
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

func (n *Node) Hash() string {
	if len(n.Metadata) == 0 {
		// It happens for empty file
		return "69217a3079908094e11121d042354a7c1f55b6482ca1a51e1b250dfd1ed0eef9"
	}
	return n.Metadata["blake2b-hash"].(string)
}

func (n *Node) IsDir() bool {
	return n.Type == rnode.Dir
}

func (n *Node) IsFile() bool {
	return n.Type == rnode.File
}

func (n *Node) Mtime() uint64 {
	if n.ModTime != "" {
		t, err := time.Parse(time.RFC3339, n.ModTime)
		if err != nil {
			panic(err)
		}
		return uint64(t.Unix())
	}
	return 0
}

func (n *Node) Ctime() uint64 {
	if n.ChangeTime != "" {
		t, err := time.Parse(time.RFC3339, n.ChangeTime)
		if err != nil {
			panic(err)
		}
		return uint64(t.Unix())
	}
	return 0
}
