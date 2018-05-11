package oplog // import "a4.io/blobstash/pkg/client/oplog"

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"net/http"

	"a4.io/blobstash/pkg/client/clientutil"
)

var (
	headerEvent = []byte("event:")
	headerData  = []byte("data:")
)

type Oplog struct {
	client *clientutil.ClientUtil
}

type Op struct {
	Event string
	Data  string
}

func New(client *clientutil.ClientUtil) *Oplog {
	return &Oplog{client}
}

// Get fetch the given blob from the remote BlobStash instance.
func (o *Oplog) GetBlob(ctx context.Context, hash string) ([]byte, error) {
	resp, err := o.client.Get(fmt.Sprintf("/api/blobstore/blob/%s", hash))
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if err := clientutil.ExpectStatusCode(resp, http.StatusOK); err != nil {
		if err.IsNotFound() {
			return nil, clientutil.ErrBlobNotFound
		}
		return nil, err
	}

	return clientutil.Decode(resp)
}

// FIXME(tsileo): use a ctx and support cancelation
func (o *Oplog) Notify(ctx context.Context, ops chan<- *Op, connCallback func()) error {
	resp, err := o.client.Get("/_oplog/")
	if err != nil {
		return err
	}

	if err := clientutil.ExpectStatusCode(resp, http.StatusOK); err != nil {
		return err
	}

	if connCallback != nil {
		connCallback()
	}

	reader := bufio.NewReader(resp.Body)

	defer resp.Body.Close()
	// TODO(tsileo): a select with ctx cancel?
	var op *Op
	for {
		// Read each new line and process the type of event
		line, err := reader.ReadBytes('\n')
		if err != nil {
			return err
		}
		switch {
		case bytes.HasPrefix(line, headerEvent):
			if op == nil {
				op = &Op{}
			}
			// Remove header
			event := bytes.Replace(line, headerEvent, []byte(""), 1)
			op.Event = string(event[1 : len(event)-1]) // Remove initial space and newline
		case bytes.HasPrefix(line, headerData):
			if op == nil {
				op = &Op{}
			}
			// Remove header
			data := bytes.Replace(line, headerData, []byte(""), 1)
			op.Data = string(data[1 : len(data)-1]) // Remove initial space and newline
		default:
			if op != nil {
				// Skip the heartbeat
				if op.Event != "heartbeat" {
					ops <- op
				}
				op = nil
			}
		}
	}
}
