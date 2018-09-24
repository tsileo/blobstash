package filetree // import "a4.io/blobstash/pkg/client/filetree"

import (
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
