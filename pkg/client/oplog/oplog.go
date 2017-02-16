package oplog // import "a4.io/blobstash/pkg/client/oplog"

import (
	"bufio"
	"bytes"
	"fmt"
	"io/ioutil"

	"a4.io/blobstash/pkg/client/clientutil"
)

var (
	headerEvent = []byte("event:")
	headerData  = []byte("data:")
)

// FIXME(tsileo): move this to client util
var defaultServerAddr = "http://localhost:8050"
var defaultUserAgent = "Oplog Go client v1"

type Oplog struct {
	client *clientutil.Client
}

type Op struct {
	Event string
	Data  string
}

// FIXME(tsileo): same here, move this to client util
func DefaultOpts() *clientutil.Opts {
	return &clientutil.Opts{
		Host:              defaultServerAddr,
		UserAgent:         defaultUserAgent,
		APIKey:            "",
		EnableHTTP2:       true,
		SnappyCompression: false,
	}
}

func New(opts *clientutil.Opts) *Oplog {
	if opts == nil {
		opts = DefaultOpts()
	}
	return &Oplog{
		client: clientutil.New(opts),
	}
}

// Get fetch the given blob from the remote BlobStash instance.
func (o *Oplog) GetBlob(hash string) ([]byte, error) {
	resp, err := o.client.DoReq("GET", fmt.Sprintf("/api/blobstore/blob/%s", hash), nil, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode == 200:
		sr := clientutil.NewSnappyResponseReader(resp)
		defer sr.Close()
		return ioutil.ReadAll(sr)
	case resp.StatusCode == 404:
		return nil, fmt.Errorf("Blob %s not found", hash)
	default:
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("failed to get blob %v: %v", hash, string(body))
	}
}

// FIXME(tsileo): use a ctx and support cancelation
func (o *Oplog) Notify(ops chan<- *Op) error {
	resp, err := o.client.DoReq("GET", "/_oplog/", nil, nil)
	if err != nil {
		return err
	}

	if resp.StatusCode != 200 {
		return fmt.Errorf("bad status code: %d", resp.StatusCode)
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

	return nil
}
