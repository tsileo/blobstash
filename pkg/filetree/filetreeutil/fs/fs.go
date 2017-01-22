package fs // import "a4.io/blobstash/pkg/filetree/filetreeutil/fs"

import (
	"encoding/json"
)

var FSKeyFmt = "filetree:fs:%v"

type FS struct {
	Name     string                 `json:"name"`
	Hostname string                 `json:"hostname,omitempty"` // TODO(tsileo): remove
	Ref      string                 `json:"ref"`
	Data     map[string]interface{} `json:"data,omitempty"`
}

func New(name, ref string) *FS {
	return &FS{
		Name: name,
		Ref:  ref,
	}
}

func NewFromJSON(data []byte) (*FS, error) {
	fs := &FS{}
	if err := json.Unmarshal(data, fs); err != nil {
		return nil, err
	}
	return fs, nil
}

func (fs *FS) Mutate(ref string) error {
	fs.Ref = ref
	_, err := json.Marshal(fs)
	if err != nil {
		return err
	}
	// TODO(tsileo): rewrite me
	// if _, err := fs.vkv.Put(fmt.Sprintf(FSKeyFmt, fs.Name), string(js), -1, ""); err != nil {
	// 	return err
	// }
	return nil
}
