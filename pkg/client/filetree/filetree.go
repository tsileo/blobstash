package filetree // import "a4.io/blobstash/pkg/client/filetree"

import (
	"fmt"
	"net/http"
	"os"

	"a4.io/blobstash/pkg/client/clientutil"
)

type Filetree struct {
	client *clientutil.ClientUtil
}

// serverAddr should't have a trailing space
func New(client *clientutil.ClientUtil) *Filetree {
	return &Filetree{
		client: client,
	}
}

type snapReq struct {
	FS       string `json:"fs"`
	Message  string `json:"message"`
	Hostname string `json:"hostname"`
}

type snapResp struct {
	Version int64  `json:"version"`
	Ref     string `json:"ref"`
}

func (f *Filetree) MakeSnapshot(ref, fs, message string) (int64, error) {
	h, err := os.Hostname()
	if err != nil {
		return 0, err
	}

	s := &snapReq{FS: fs, Message: message, Hostname: h}
	resp, err := f.client.PostJSON(fmt.Sprintf("/api/filetree/node/%s/_snapshot", ref), s)
	if err != nil {
		return 0, err
	}

	if err := clientutil.ExpectStatusCode(resp, http.StatusOK); err != nil {
		return 0, err
	}

	snap := &snapResp{}
	if err := clientutil.Unmarshal(resp, snap); err != nil {
		return 0, err
	}

	return snap.Version, nil
}

func (f *Filetree) GC(ns, name string, rev int64) error {
	gcScript := fmt.Sprintf(`
local kvstore = require('kvstore')

local key = "_filetree:fs:%s"
local version = "%d"
local _, ref, _ = kvstore.get(key, version)

-- mark the actual KV entry
mark_kv(key, version)

-- mark the whole tree
mark_filetree_node(ref)
`, name, rev)

	resp, err := f.client.PostMsgpack(
		fmt.Sprintf("/api/stash/%s/_gc", ns),
		map[string]interface{}{
			"script": gcScript,
		},
	)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if err := clientutil.ExpectStatusCode(resp, http.StatusNoContent); err != nil {
		return err
	}

	return nil
}
