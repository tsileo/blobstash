package response // import "a4.io/blobstash/pkg/client/response"

// KeyValue holds a singke key value pair, along with the version (the creation timestamp)
type KeyValue struct {
	Key     string `json:"key,omitempty"`
	Hash    string `json:"hash"`
	Data    []byte `json:"data"`
	Version int    `json:"version"`
}

// KeyValueVersions holds the full history for a key value pair
type KeyValueVersions struct {
	Key      string      `json:"key"`
	Versions []*KeyValue `json:"versions"`
}

// KeysResponse is a wrapper for a list of key value pairs
type KeysResponse struct {
	Keys []*KeyValue `json:"keys"`
}
