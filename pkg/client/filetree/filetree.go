package filetree // import "a4.io/blobstash/pkg/client/filetree"

import (
	"fmt"
	"net/http"
	"os"

	"a4.io/blobstash/pkg/client/clientutil"
)

// Filetree client
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
	FS        string `json:"fs"`
	Message   string `json:"message"`
	Hostname  string `json:"hostname"`
	UserAgent string `json:"user_agent"`
}

type snapResp struct {
	Version int64  `json:"version"`
	Ref     string `json:"ref"`
}

// MakeSnaphot create a FS snapshot from a tree reference
func (f *Filetree) MakeSnapshot(ref, fs, message, userAgent string) (int64, error) {
	h, err := os.Hostname()
	if err != nil {
		return 0, err
	}

	s := &snapReq{
		FS:        fs,
		Message:   message,
		Hostname:  h,
		UserAgent: userAgent,
	}
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

// GC performs a garbage collection to save the latest filetreee snapshot
func (f *Filetree) GC(ns, name string, rev int64) error {
	resp, err := f.client.PostJSON(
		fmt.Sprintf("/api/stash/%s/_merge_filetree_version", ns),
		map[string]interface{}{
			"ref":     fmt.Sprintf("_filetree:fs:%s", name),
			"version": rev,
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
