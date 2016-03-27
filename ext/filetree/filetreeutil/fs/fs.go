package fs

import (
	"encoding/json"
)

type FS struct {
	Name     string                 `json:"name"`
	Hostname string                 `json:"hostname,omitempty"` // TODO(tsileo): remove
	Ref      string                 `json:"ref"`
	Data     map[string]interface{} `json:"data,omitempty"`
}

func New(ref string) *FS {
	return &FS{
		Ref: ref,
	}
}

func NewFromJSON(data []byte) (*FS, error) {
	fs := &FS{}
	if err := json.Unmarshal(data, fs); err != nil {
		return nil, err
	}
	return fs, nil
}
